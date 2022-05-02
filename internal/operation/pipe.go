package operation

import "cesium/shut"

type Transform[K comparable, I Operation[K], O Operation[K]] interface {
	Exec([]I) []O
}

func PipeTransform[K comparable, I Operation[K], O Operation[K]](req <-chan []I, sd shut.Shutdown, batch Transform[K, I, O]) <-chan []O {
	res := make(chan []O)
	sd.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				close(res)
				return nil
			case ops := <-req:
				res <- batch.Exec(ops)
			}
		}
	})
	return res
}

type Executor[K comparable, I Operation[K]] interface {
	Exec([]I)
}

func PipeExec[K comparable, I Operation[K]](req <-chan []I, exec Executor[K, I], sd shut.Shutdown) {
	sd.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case ops := <-req:
				exec.Exec(ops)
			}
		}
	})
}
