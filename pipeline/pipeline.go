package pipeline

type Pipeline interface {
	Start()
	Stop()
	RunOnce()
}
