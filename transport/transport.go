package transport

type Transport interface {
	Send(m []byte)
	Recv() []byte
}
