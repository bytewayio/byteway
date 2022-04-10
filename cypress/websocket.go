package cypress

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WebSocketSession a connected web socket session
type WebSocketSession struct {
	RemoteAddr   string
	User         *UserPrincipal
	Session      *Session
	Context      map[string]interface{}
	connection   *websocket.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLock     *sync.Mutex
	writeLock    *sync.Mutex
}

// Close close the underlying connection of the WebSocketSession
func (session *WebSocketSession) Close() error {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()
	return session.connection.Close()
}

// SendTextMessage sends a text message to the remote, a more convenient version of SendMessage
func (session *WebSocketSession) SendTextMessage(text string) error {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()
	session.resetWriteTimeout()
	return session.connection.WriteMessage(websocket.TextMessage, []byte(text))
}

// SendBinaryMessage sends a binary message to the remote, a more convenient version of SendMessage
func (session *WebSocketSession) SendBinaryMessage(data []byte) error {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()
	session.resetWriteTimeout()
	return session.connection.WriteMessage(websocket.BinaryMessage, data)
}

// SendMessage sends a message with the given type
func (session *WebSocketSession) SendMessage(msgType int, data []byte) error {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()
	session.resetWriteTimeout()
	return session.connection.WriteMessage(msgType, data)
}

func (session *WebSocketSession) resetReadTimeout() {
	session.readLock.Lock()
	defer session.readLock.Unlock()
	if session.readTimeout > time.Duration(0) {
		session.connection.SetReadDeadline(time.Now().Add(session.readTimeout))
	}
}

func (session *WebSocketSession) resetWriteTimeout() {
	if session.writeTimeout > time.Duration(0) {
		session.connection.SetWriteDeadline(time.Now().Add(session.writeTimeout))
	}
}

//WebSocketListener web socket listener that could be used to listen on a specific web socket endpoint
type WebSocketListener interface {
	// OnConnect when a connection is established
	OnConnect(session *WebSocketSession)

	// OnTextMessage when a text message is available in the channel
	OnTextMessage(session *WebSocketSession, text string)

	// OnBinaryMessage when a binary message is available in the channel
	OnBinaryMessage(session *WebSocketSession, data []byte)

	// OnClose when the channel is broken or closed by remote
	OnClose(session *WebSocketSession, reason int)
}

// KeepAliveMessageHandler websocket message handler to handle ping/pong messages
// for ping message, a pong message will be sent back after OnPingMessage is returned.
type KeepAliveMessageHandler interface {
	// OnPingMessage when a ping message is received, no need to send back pong message,
	// which is done right after this handler
	OnPingMessage(session *WebSocketSession, text string) error

	// OnPongMessage when a pong message is received, this API will be called to notify the application
	OnPongMessage(session *WebSocketSession, text string) error
}

var upgrader = websocket.Upgrader{}

// WebSocketHandler Web socket handler
// have handler.Handle for router to enable web socket endpoints
type WebSocketHandler struct {
	MessageLimit     int64
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	Listener         WebSocketListener
	WriteCompression bool
	CheckOrigin      func(*http.Request) bool
}

// Handle handles the incomping web requests and try to upgrade the request into a websocket connection
func (handler *WebSocketHandler) Handle(writer http.ResponseWriter, request *http.Request) {
	if handler.CheckOrigin != nil {
		upgrader.CheckOrigin = handler.CheckOrigin
	}

	conn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		zap.L().Error("failed to upgrade the incoming connection to a websocket", zap.Error(err), zap.String("remoteAddr", request.RemoteAddr))
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("<h1>Bad request</h1>"))
		return
	}

	var userPrincipal *UserPrincipal
	var session *Session
	contextValue := request.Context().Value(SessionKey)
	if contextValue != nil {
		var ok bool
		session, ok = contextValue.(*Session)
		if !ok {
			zap.L().Error("invalid session object in SessionKey", zap.String("remoteAddr", request.RemoteAddr))
			writer.WriteHeader(http.StatusInternalServerError)
			writer.Write([]byte("<h1>bad server configuration</h1>"))
			return
		}
	}

	if session == nil {
		zap.L().Error("session handler is required for websocket handler", zap.String("remoteAddr", request.RemoteAddr))
		writer.WriteHeader(http.StatusServiceUnavailable)
		writer.Write([]byte("<h1>A http session is required</h1>"))
		return
	}

	contextValue = request.Context().Value(UserPrincipalKey)
	if contextValue != nil {
		userPrincipal, _ = contextValue.(*UserPrincipal)
	}

	if handler.MessageLimit > 0 {
		conn.SetReadLimit(handler.MessageLimit)
	}

	if handler.WriteCompression {
		conn.EnableWriteCompression(true)
	}

	webSocketSession := &WebSocketSession{
		request.RemoteAddr,
		userPrincipal,
		session,
		make(map[string]interface{}),
		conn,
		handler.ReadTimeout,
		handler.WriteTimeout,
		&sync.Mutex{},
		&sync.Mutex{},
	}

	for key, value := range request.URL.Query() {
		webSocketSession.Context[key] = value
	}

	handler.Listener.OnConnect(webSocketSession)
	go handler.connectionLoop(webSocketSession)
}

func (handler *WebSocketHandler) connectionLoop(session *WebSocketSession) {
	if h, ok := handler.Listener.(KeepAliveMessageHandler); ok {
		session.connection.SetPingHandler(func(pingData string) error {
			session.resetReadTimeout()
			err := h.OnPingMessage(session, pingData)
			if err != nil {
				zap.L().Error("handle ping message failed", zap.Error(err), zap.String("remoteAddr", session.RemoteAddr))
				return err
			}

			err = session.SendMessage(websocket.PongMessage, []byte(pingData))
			if err != nil {
				zap.L().Error("not able to write pong message back", zap.Error(err), zap.String("remoteAddr", session.RemoteAddr))
			}

			return err
		})
		session.connection.SetPongHandler(func(pongData string) error {
			session.resetReadTimeout()
			return h.OnPongMessage(session, pongData)
		})
	}

	for {
		session.resetReadTimeout()
		msgType, data, err := session.connection.ReadMessage()
		if err != nil {
			zap.L().Error("failed to read from ws peer", zap.Error(err), zap.String("remoteAddr", session.RemoteAddr))
			handler.Listener.OnClose(session, websocket.CloseAbnormalClosure)
			session.connection.Close()
			return
		}

		switch msgType {
		case websocket.BinaryMessage:
			handler.Listener.OnBinaryMessage(session, data)
		case websocket.TextMessage:
			handler.Listener.OnTextMessage(session, string(data))
		case websocket.CloseMessage:
			handler.Listener.OnClose(session, websocket.CloseNormalClosure)
			session.connection.Close()
			return
		default:
			zap.L().Error("not able to handle message type", zap.Int("messageType", msgType), zap.String("remoteAddr", session.RemoteAddr))
		}
	}
}
