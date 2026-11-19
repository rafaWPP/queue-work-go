// common/job.go
package common

// Job é a interface que todos os jobs devem implementar.
type Job interface {
	Handle() error
}
