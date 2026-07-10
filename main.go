// Command mpos-mock is a local TCP server that mimics Epoint/MPOS framing for development.
// It prints (and optionally serves over HTTP) the supported [CMD] values, returns canned
// success responses, and can delay each reply via -latency or MPOS_MOCK_LATENCY.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Canned bodies are adapted from parser / api tests so downstream parsing tends to succeed.
const (
	respENQSTATUS = "##[CMD]ENQSTATUS[RESPONSE]BILLOPEN[BILLNO]A200270##"

	respENQDETAILS = "##[CMD]ENQDETAILS[MERCHCODE]MERCH[APPID]QLUB[TABLENO]50[TERMINAL]T01[RESPONSE]BILLOPEN[BILLNO]A200270[PAX]2[CUSTCODE]CUST123[SUBTOTAL]10.00[TOTAL]10.50[AMTDUE]10.50[FOOTERREM]0.50[ITEMNO]1[ITEMCODE]ITEM1[QUANTITY]1.00[ITEMDESC]Description[ITEMAMT]10.00[ITEMDISC]0.00##"

	respENQITEMSOLDOUT = "##[CMD]ENQITEMSOLDOUT[MERCHCODE]MOCK[APPID]QLUB[RESPONSE]ACCEPTED##"

	respGETORDERTTL = "##[CMD]GETORDERTTL[MERCHCODE]SG_DOTASSTC[APPID]QLUB[SECRETKEY3]1227682e6560c4cbcade6698ca307418e990f11e[TABLENO]8[PAX]1[TERMINAL]QB01[RESPONSE]ACCEPTED[SUBTOTAL]1.99[TOTAL]2.39[AMTDUE]2.39%%[ITEMREFNO]1[ITEMCODE]A01003[SUBITEM]N[QUANTITY]1.00[ITEMPRICE]1.99[ITEMDESC]N03 P'thani CX RN  $$[PAYMTYPE]ADDTAX1[PAYMDESC]SVC CHG 10%[AMOUNT]0.20$$[PAYMTYPE]ADDTAX2[PAYMDESC]GST 9%[AMOUNT]0.20 ##"

	respADDORDER = "##[CMD]ADDORDER[MERCHCODE]TEST[RESPONSE]ACCEPTED[BILLNO]B123[TRANSID]123456##"

	respUnknown = "##[RESPONSE]ACCEPTED##"

	defaultTCPAddr = "127.0.0.1:39103"
)

type apiEntry struct {
	CMD             string `json:"cmd"`
	ClientMethod    string `json:"clientMethod"`
	Description     string `json:"description"`
	ResponseLength  int    `json:"responseLength"`
	ResponseSnippet string `json:"responseSnippet"`
}

func registry() []apiEntry {
	entries := []struct {
		cmd, method, desc, body string
	}{
		{"ENQSTATUS", "CheckTable", "Table status (ENQSTATUS)", respENQSTATUS},
		{"ENQDETAILS", "CheckTableDetails", "Bill / line items (ENQDETAILS)", respENQDETAILS},
		{"ENQITEMSOLDOUT", "GetOutStockItems", "Sold-out items list", respENQITEMSOLDOUT},
		{"GETORDERTTL", "GetOrderTotal", "Pre-submit totals (GETORDERTTL)", respGETORDERTTL},
		{"ADDORDER", "PayOrder / CloseOrder / SubmitOrder", "Payment or order submit (ADDORDER)", respADDORDER},
	}
	out := make([]apiEntry, 0, len(entries))
	for _, e := range entries {
		snip := e.body
		if len(snip) > 120 {
			snip = snip[:120] + "…"
		}
		out = append(out, apiEntry{
			CMD:             e.cmd,
			ClientMethod:    e.method,
			Description:     e.desc,
			ResponseLength:  len(e.body),
			ResponseSnippet: snip,
		})
	}
	return out
}

func responseForCMD(cmd string) string {
	switch cmd {
	case "ENQSTATUS":
		return respENQSTATUS
	case "ENQDETAILS":
		return respENQDETAILS
	case "ENQITEMSOLDOUT":
		return respENQITEMSOLDOUT
	case "GETORDERTTL":
		return respGETORDERTTL
	case "ADDORDER":
		return respADDORDER
	default:
		return respUnknown
	}
}

func detectCMD(payload string) string {
	// Order: longer / more specific tags first where relevant.
	switch {
	case strings.Contains(payload, "[CMD]ENQDETAILS"):
		return "ENQDETAILS"
	case strings.Contains(payload, "[CMD]ENQSTATUS"):
		return "ENQSTATUS"
	case strings.Contains(payload, "[CMD]ENQITEMSOLDOUT"):
		return "ENQITEMSOLDOUT"
	case strings.Contains(payload, "[CMD]GETORDERTTL"):
		return "GETORDERTTL"
	case strings.Contains(payload, "[CMD]ADDORDER"):
		return "ADDORDER"
	default:
		return ""
	}
}

// readRequest reads the client request bytes. The MPOS client keeps the connection open
// for the reply, so we idle-timeout between reads instead of waiting for EOF.
func readRequest(conn net.Conn, overall time.Duration) ([]byte, error) {
	deadline := time.Now().Add(overall)
	var buf bytes.Buffer
	b := make([]byte, 8192)
	idle := 500 * time.Millisecond
	const maxReqSize = 256 * 1024

	for {
		if time.Now().After(deadline) {
			if buf.Len() > 0 {
				return buf.Bytes(), nil
			}
			return nil, os.ErrDeadlineExceeded
		}

		next := time.Now().Add(idle)
		if next.After(deadline) {
			next = deadline
		}
		_ = conn.SetReadDeadline(next)

		n, err := conn.Read(b)
		if n > 0 {
			_, _ = buf.Write(b[:n])
			payload := buf.Bytes()
			if bytes.Contains(payload, []byte("[CMD]")) && bytes.HasSuffix(payload, []byte("##")) {
				return payload, nil
			}
			if buf.Len() >= maxReqSize {
				return payload, nil
			}
		}

		if n == 0 && err == nil {
			if buf.Len() > 0 {
				return buf.Bytes(), nil
			}
			continue
		}

		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if buf.Len() > 0 {
					return buf.Bytes(), nil
				}
				continue
			}
			if buf.Len() > 0 {
				// EOF / timeout after partial read should still be treated as a complete request.
				return buf.Bytes(), nil
			}
			return nil, err
		}
	}
}

// parseResetCMDs builds the set of CMDs whose connections are reset (TCP RST)
// instead of answered, e.g. "ADDORDER" or "ADDORDER,GETORDERTTL". Flag wins over env.
func parseResetCMDs(flagVal, envVal string) map[string]bool {
	raw := strings.TrimSpace(flagVal)
	if raw == "" {
		raw = strings.TrimSpace(envVal)
	}
	set := make(map[string]bool)
	for _, c := range strings.Split(raw, ",") {
		if c = strings.ToUpper(strings.TrimSpace(c)); c != "" {
			set[c] = true
		}
	}
	return set
}

// abortConn closes the connection with SO_LINGER=0 so the kernel sends RST instead
// of FIN; the client's pending read then fails with "connection reset by peer".
func abortConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

func parseLatency(flagVal, envVal string) (time.Duration, error) {
	if strings.TrimSpace(flagVal) != "" {
		return time.ParseDuration(strings.TrimSpace(flagVal))
	}
	if strings.TrimSpace(envVal) != "" {
		return time.ParseDuration(strings.TrimSpace(envVal))
	}
	return 0, nil
}

// effectiveTCPListenAddr picks the TCP bind address: MPOS_MOCK_TCP_ADDR overrides;
// otherwise -addr; if still the local default and PORT is set (e.g. Railway), bind 0.0.0.0:PORT.
func effectiveTCPListenAddr(flagAddr string) string {
	if v := strings.TrimSpace(os.Getenv("MPOS_MOCK_TCP_ADDR")); v != "" {
		return v
	}
	if flagAddr != defaultTCPAddr {
		return flagAddr
	}
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		return "0.0.0.0:" + p
	}
	return flagAddr
}

func railwayLogSuffix() string {
	var parts []string
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		parts = append(parts, "env_PORT="+p)
	}
	if d := strings.TrimSpace(os.Getenv("RAILWAY_PUBLIC_DOMAIN")); d != "" {
		parts = append(parts, "RAILWAY_PUBLIC_DOMAIN="+d)
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func tcpBindHostPort(addr net.Addr) (host, port string) {
	if ta, ok := addr.(*net.TCPAddr); ok {
		switch {
		case ta.IP == nil || ta.IP.IsUnspecified():
			// Kernel may report [::] for a 0.0.0.0 bind (dual-stack); log one stable form for operators.
			host = "0.0.0.0"
		default:
			host = ta.IP.String()
			if ta.Zone != "" {
				host += "%" + ta.Zone
			}
		}
		return host, strconv.Itoa(ta.Port)
	}
	h, p, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String(), ""
	}
	if h == "" {
		h = "0.0.0.0"
	}
	return h, p
}

func listenHostPort(listen string) (host, port string) {
	listen = strings.TrimSpace(listen)
	if listen == "" {
		return "", ""
	}
	if strings.HasPrefix(listen, ":") {
		return "0.0.0.0", strings.TrimPrefix(listen, ":")
	}
	h, p, err := net.SplitHostPort(listen)
	if err != nil {
		return listen, ""
	}
	if h == "" {
		h = "0.0.0.0"
	}
	return h, p
}

func printTable(entries []apiEntry) {
	fmt.Println("mpos-mock: supported MPOS [CMD] handlers (TCP success bodies)")
	fmt.Println(strings.Repeat("-", 120))
	fmt.Printf("%-14s %-36s %-28s %s\n", "CMD", "Client (api)", "Description", "Response (len / snippet)")
	fmt.Println(strings.Repeat("-", 120))
	for _, e := range entries {
		fmt.Printf("%-14s %-36s %-28s len=%d %q\n", e.CMD, e.ClientMethod, e.Description, e.ResponseLength, e.ResponseSnippet)
	}
	fmt.Println(strings.Repeat("-", 120))
}

func main() {
	// Railway (and similar) classify stderr as "error" level; route default logger to stdout for normal ops.
	log.SetOutput(os.Stdout)

	addr := flag.String("addr", defaultTCPAddr, "TCP listen address (overridden by MPOS_MOCK_TCP_ADDR; default + PORT -> 0.0.0.0:PORT for Railway)")
	latencyFlag := flag.String("latency", "", "delay before each TCP response (e.g. 500ms, 2s). Empty uses MPOS_MOCK_LATENCY or 0")
	httpAddr := flag.String("http", "", "optional HTTP listen address for GET / JSON API list (e.g. :8089)")
	resetFlag := flag.String("reset-cmds", "", "comma-separated CMDs answered with a TCP reset instead of a response, e.g. ADDORDER (env: MPOS_MOCK_RESET_CMDS)")
	flag.Parse()

	latency, err := parseLatency(*latencyFlag, os.Getenv("MPOS_MOCK_LATENCY"))
	if err != nil {
		log.Fatalf("invalid latency: %v", err)
	}
	resetCMDs := parseResetCMDs(*resetFlag, os.Getenv("MPOS_MOCK_RESET_CMDS"))

	startTime := time.Now().UTC()
	tcpListenAddr := effectiveTCPListenAddr(*addr)
	railwayExtra := railwayLogSuffix()

	reg := registry()
	printTable(reg)

	if *httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("ok"))
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			if err := enc.Encode(reg); err != nil {
				log.Printf("http encode: %v", err)
			}
		})
		httpHost, httpPort := listenHostPort(*httpAddr)
		go func() {
			log.Printf("mpos-mock http_start service_start=%s http_host=%s http_port=%s bind_address=%q%s",
				startTime.Format(time.RFC3339Nano), httpHost, httpPort, *httpAddr, railwayExtra)
			if err := http.ListenAndServe(*httpAddr, mux); err != nil {
				log.Fatalf("HTTP server: %v", err)
			}
		}()
	}

	ln, err := net.Listen("tcp", tcpListenAddr)
	if err != nil {
		log.Fatalf("TCP listen %q: %v", tcpListenAddr, err)
	}
	tcpHost, tcpPort := tcpBindHostPort(ln.Addr())
	log.Printf("mpos-mock tcp_start service_start=%s tcp_host=%s tcp_port=%s bind_address=%q latency=%v reset_cmds=%v%s",
		startTime.Format(time.RFC3339Nano), tcpHost, tcpPort, tcpListenAddr, latency, resetKeys(resetCMDs), railwayExtra)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, latency, resetCMDs)
	}
}

func resetKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

func handleConn(conn net.Conn, latency time.Duration, resetCMDs map[string]bool) {
	defer conn.Close()

	req, err := readRequest(conn, 30*time.Second)
	if err != nil {
		log.Printf("read: %v (replying generic ACCEPTED)", err)
		body := responseForCMD("")
		if latency > 0 {
			time.Sleep(latency)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, writeErr := conn.Write([]byte(body)); writeErr != nil {
			log.Printf("write fallback: %v", writeErr)
		}
		return
	}
	cmd := detectCMD(string(req))
	if cmd != "" && resetCMDs[cmd] {
		if latency > 0 {
			time.Sleep(latency)
		}
		log.Printf("cmd=%s -> injecting TCP reset (connection reset by peer)", cmd)
		abortConn(conn)
		return
	}
	body := responseForCMD(cmd)
	if cmd == "" {
		log.Printf("unknown CMD in request (%d bytes), replying generic ACCEPTED", len(req))
	} else {
		log.Printf("cmd=%s -> canned response len=%d", cmd, len(body))
	}

	if latency > 0 {
		time.Sleep(latency)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(body)); err != nil {
		log.Printf("write: %v", err)
	}
}
