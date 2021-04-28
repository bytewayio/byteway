package cypress

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/websocket"
)

type TestController struct {
}

func (c *TestController) Action1(req *http.Request, resp *Response) {
	resp.DoneWithContent(200, "text/plain", []byte("action1"))
}

func TestAutoController(t *testing.T) {
	tc := &TestController{}
	actions := AsController(tc)()
	if len(actions) != 1 {
		t.Error("expected one action in the list but got", len(actions))
		return
	}
}

type TestUserProvider struct{}

func (p *TestUserProvider) GetName() string {
	return "testProvider"
}

func (p *TestUserProvider) Authenticate(r *http.Request) *UserPrincipal {
	ticket := r.URL.Query().Get("ticket")
	if ticket != "" {
		return &UserPrincipal{
			ID:     ticket,
			Name:   ticket,
			Domain: "test",
			Roles:  make([]string, 0),
		}
	}

	return nil
}

func (p *TestUserProvider) Load(domain, id string) *UserPrincipal {
	return &UserPrincipal{
		ID:     id,
		Domain: domain,
		Name:   id,
		Roles:  make([]string, 0),
	}
}

type TestWsListener struct{}

func (l *TestWsListener) OnConnect(session *WebSocketSession) {
	fmt.Println("a websocket with session id", session.Session.ID, "is connected")
}

func (l *TestWsListener) OnClose(session *WebSocketSession, reason int) {
	fmt.Println("a websocket with session id", session.Session.ID, "has closed with reason", reason)
}

func (l *TestWsListener) OnTextMessage(session *WebSocketSession, message string) {
	err := session.SendTextMessage(message)
	if err != nil {
		fmt.Println("failed to send message due to error", err)
	}
}

func (l *TestWsListener) OnBinaryMessage(session *WebSocketSession, message []byte) {
	err := session.SendBinaryMessage(message)
	if err != nil {
		fmt.Println("failed to send message due to error", err)
	}
}

func (l *TestWsListener) OnPingMessage(session *WebSocketSession, text string) error {
	fmt.Println("Received a ping message", text)
	return nil
}

func (l *TestWsListener) OnPongMessage(session *WebSocketSession, text string) error {
	fmt.Println("Received a pong message", text)
	return nil
}

func testActions(t *testing.T) []Action {
	actions := []Action{
		{
			Name: "greeting",
			Handler: ActionHandler(func(r *http.Request, response *Response) {
				response.DoneWithContent(http.StatusAccepted, "text/html", []byte(fmt.Sprintf("<h1>Hello, %s</h1>", r.URL.String())))

				session, _ := r.Context().Value(SessionKey).(*Session)
				if session != nil {
					fmt.Println("SESSID:", session.ID)
				} else {
					t.Error("no session detected while one expected")
				}
			}),
		},
		{
			Name: "timeout",
			Handler: ActionHandler(func(r *http.Request, response *Response) {
				var requestDone uint32
				select {
				case <-r.Context().Done():
					if r.Context().Err() == context.DeadlineExceeded {
						if atomic.CompareAndSwapUint32(&requestDone, 0, 1) {
							response.DoneWithError(500, "server timeout")
						}
					}
					break
				case <-time.After(time.Second * 3):
					if atomic.CompareAndSwapUint32(&requestDone, 0, 1) {
						response.DoneWithContent(200, "text/plain", []byte("ok"))
					}
					break
				}
			}),
		},
		{
			Name: "panic",
			Handler: ActionHandler(func(r *http.Request, response *Response) {
				panic("ask for panic")
			}),
		},
		{
			Name: "index",
			Handler: ActionHandler(func(request *http.Request, response *Response) {
				model := &TestModel{
					Title:   "Page Title",
					Message: "Page Content",
				}

				response.DoneWithTemplate(http.StatusOK, "index", model)
			}),
		},
	}

	return actions[:]
}

func printSessionID(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		traceID := GetTraceID(request.Context())
		session := GetSession(request)
		fmt.Println("printSessionID:", traceID, session.ID)
		handler.ServeHTTP(writer, request)
	})
}

func printSessionIDEx(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		traceID := GetTraceID(request.Context())
		session := GetSession(request)
		fmt.Println("printSessionIDEx:", traceID, session.ID)
		handler.ServeHTTP(writer, request)
	})
}

func DumpBufferWriter(t *testing.T, writer *BufferedWriter) {
	for _, buf := range writer.Buffer {
		t.Log(string(buf))
	}
}

func TestWebServer(t *testing.T) {
	// test setup
	// create test folder
	testDir, err := ioutil.TempDir("", "cytpltest")
	if err != nil {
		t.Error("failed to create test dir", err)
		return
	}

	defer os.RemoveAll(testDir)

	// write template files
	err = ioutil.WriteFile(path.Join(testDir, "header.tmpl"), []byte("{{define \"header\"}}{{.}}{{end}}"), os.ModePerm)
	if err != nil {
		t.Error("failed to setup header.tmpl")
		return
	}

	err = ioutil.WriteFile(path.Join(testDir, "index.tmpl"), []byte("{{define \"index\"}}{{template \"header\" .Title}}{{.Message}}{{add 1 1}}{{end}}"), os.ModePerm)
	if err != nil {
		t.Error("failed to setup index.tmpl")
		return
	}

	writer := NewBufferWriter()
	SetupLogger(LogLevelDebug, writer)
	tmplMgr := NewTemplateManager(testDir, ".tmpl", time.Second*10, func(root *template.Template) {
		root.Funcs(template.FuncMap{
			"add": func(a, b int) int {
				return a + b
			},
		})
	}, func(path string) bool {
		return strings.HasSuffix(path, "header.tmpl")
	})
	defer tmplMgr.Close()
	server := NewWebServer(":8099", NewSkinManager(tmplMgr))
	defer server.Shutdown()

	sessionStore := NewInMemorySessionStore()
	defer sessionStore.Close()

	server.AddUserProvider(&TestUserProvider{})
	server.AddUserProvider(NewJwtUserProvider(JwtKeyMap(make(map[string]*rsa.PublicKey)), nil))
	server.WithSessionOptions(sessionStore, 15*time.Minute)
	server.WithRequestTimeout(2)
	server.WithStandardRouting("/web")
	server.WithCaptcha("/captcha")
	server.AddWsEndoint("/ws/echo", &TestWsListener{})
	server.RegisterController("test", ControllerFunc(func() []Action { return testActions(t) }))
	server.RegisterController("test1", AsController(&TestController{}))
	server.WithCustomHandler(CustomHandlerFunc(printSessionID))
	server.WithCustomHandler(CustomHandlerFunc(printSessionIDEx))
	server.WithCORS(handlers.CORS(handlers.AllowedOrigins([]string{"*"}), handlers.AllowedMethods([]string{"*"})))

	startedChan := make(chan bool)
	go func() {
		startedChan <- true
		if err := server.Start(); err != nil {
			fmt.Println(err)
		}
	}()

	// wait for the server to start
	<-startedChan
	time.Sleep(time.Millisecond * 100)
	resp, err := http.Get("http://localhost:8099/web/test/greeting?ticket=test")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		resp.Body.Close()
		t.Error("failed to read body", err)
		DumpBufferWriter(t, writer)
		return
	}

	resp.Body.Close()
	if string(body) != "<h1>Hello, /web/test/greeting?ticket=test</h1>" {
		t.Error("unexpected response", string(body))
		DumpBufferWriter(t, writer)
		return
	}

	resp, err = http.Get("http://localhost:8099/web/test/timeout")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	type routerLog struct {
		Message    string `json:"msg"`
		Controller string `json:"controller"`
		Action     string `json:"action"`
		TraceID    string `json:"activityId"`
	}

	type apiLog struct {
		Message    string `json:"msg"`
		TraceID    string `json:"activityId"`
		URI        string `json:"requestUri"`
		Path       string `json:"path"`
		Method     string `json:"requestMethod"`
		User       string `json:"user"`
		StatusCode int    `json:"responseStatus"`
	}

	if len(writer.Buffer) != 7 {
		t.Error("expecting 7 log items but got", len(writer.Buffer))
		DumpBufferWriter(t, writer)
		return
	}

	log1 := routerLog{}
	log2 := apiLog{}
	err = json.Unmarshal(writer.Buffer[3], &log1)
	if err != nil {
		t.Error("bad log item", err)
		DumpBufferWriter(t, writer)
		return
	}

	if log1.Controller != "test" {
		t.Error("expecting test but got", log1.Controller)
		DumpBufferWriter(t, writer)
		return
	}

	if log1.Action != "greeting" {
		t.Error("expecting greeting but got", log1.Action)
		DumpBufferWriter(t, writer)
		return
	}

	err = json.Unmarshal(writer.Buffer[4], &log2)
	DumpBufferWriter(t, writer)

	if err != nil {
		t.Error("bad log item", err)
		DumpBufferWriter(t, writer)
		return
	}

	if log2.Path != "/web/test/greeting" {
		t.Error("expecting /web/test/greeting but got", log2.Path)
		DumpBufferWriter(t, writer)
		return
	}

	if log2.StatusCode != 202 {
		t.Error("expecting 202 but got", log2.StatusCode)
		DumpBufferWriter(t, writer)
		return
	}

	if log1.TraceID != log2.TraceID {
		t.Error(log1.TraceID, log2.TraceID, "expecting to be matched")
		DumpBufferWriter(t, writer)
		return
	}

	resp, err = http.Get("http://localhost:8099/web/test1/action1")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusOK {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		resp.Body.Close()
		t.Error("failed to read body", err)
		DumpBufferWriter(t, writer)
		return
	}

	resp.Body.Close()
	if string(body) != "action1" {
		t.Error("unexpected response", string(body))
		DumpBufferWriter(t, writer)
		return
	}

	resp, err = http.Get("http://localhost:8099/web/test1/action2")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	resp.Body.Close()

	resp, err = http.Get("http://localhost:8099/web/test/index")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusOK {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error("failed to read body")
		DumpBufferWriter(t, writer)
		return
	}

	if string(body) != "Page TitlePage Content2" {
		t.Error("unexpected response body", string(body))
		DumpBufferWriter(t, writer)
		return
	}

	resp, err = http.Get("http://localhost:8099/captcha?sessid=abc123")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusOK {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	sess, _ := sessionStore.Get("abc123")
	val, _ := sess.GetValue("captcha")
	fmt.Println("challenge", val.(string))

	defer resp.Body.Close()

	resp, err = http.Get("http://localhost:8099/captcha")
	if err != nil {
		t.Error("server is not started or working properly", err)
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != http.StatusOK {
		t.Error("Unexpected http status", resp.Status)
		DumpBufferWriter(t, writer)
		return
	}

	defer resp.Body.Close()

	// try websocket
	c, _, err := websocket.DefaultDialer.Dial("ws://localhost:8099/ws/echo", nil)
	if err != nil {
		t.Error("dial:", err)
		DumpBufferWriter(t, writer)
		return
	}

	defer c.Close()
	c.WriteMessage(websocket.TextMessage, []byte("Hello, websocket!"))
	msgType, msg, err := c.ReadMessage()
	if msgType != websocket.TextMessage || err != nil || string(msg) != "Hello, websocket!" {
		t.Error("failed to read back the message")
		DumpBufferWriter(t, writer)
		return
	}

	pongC := make(chan int)
	c.SetPongHandler(func(data string) error {
		fmt.Println("received a pong message", data)
		if data != "ping" {
			t.Error("Received a bad pong message")
			DumpBufferWriter(t, writer)
		}
		go func() {
			pongC <- 1
		}()
		return nil
	})
	err = c.WriteMessage(websocket.PingMessage, []byte("ping"))
	if err != nil {
		t.Error("failed to send ping message", err.Error())
		DumpBufferWriter(t, writer)
		return
	}

	go c.ReadMessage()
	<-pongC

	req, err := http.NewRequest("GET", "http://localhost:8099/web/test/index", nil)
	if err != nil {
		t.Error("failed to create request", err.Error())
		DumpBufferWriter(t, writer)
		return
	}

	req.Header.Add("Origin", "https://www.google.com")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error("failed to send request", err.Error())
		DumpBufferWriter(t, writer)
		return
	}

	if resp.StatusCode != 200 {
		t.Error("getting a non ok request from server")
		DumpBufferWriter(t, writer)
		return
	}

	allowedOrigin := resp.Header.Get("Access-Control-Allow-Origin")
	if allowedOrigin != "*" {
		t.Error("expect * as allowed origin but got", allowedOrigin)
		DumpBufferWriter(t, writer)
		return
	}

}
