package caesium_test

import (
	"caesium"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"sync"
)

var _ = Describe("Flyfs", func() {
	Describe("KeyFile lock", func() {
		It("Should l and Release the KeyFile without error", func() {
			fs := caesium.NewKFS(caesium.NewOS("testdata"))
			fpk := caesium.NewPK()
			_, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			fs.Release(fpk)
			_, err = fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			fs.Release(fpk)
			Expect(fs.Delete(fpk)).To(Succeed())
		})
		It("Should allow two goroutines to Acquire and write to a KeyFile concurrently", func() {
			fs := caesium.NewKFS(caesium.NewOS("testdata"))
			fpk := caesium.NewPK()
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				f, err := fs.Acquire(fpk)
				Expect(err).ToNot(HaveOccurred())
				_, err = f.Write([]byte("hello"))
				Expect(err).ToNot(HaveOccurred())
				fs.Release(fpk)
			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				f, err := fs.Acquire(fpk)
				Expect(err).ToNot(HaveOccurred())
				defer fs.Release(fpk)
				_, err = f.Write([]byte("world"))
				Expect(err).ToNot(HaveOccurred())
			}()
			wg.Wait()
			f, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			_, err = f.Seek(0, 0)
			Expect(err).ToNot(HaveOccurred())
			defer fs.Release(fpk)
			b, err := ioutil.ReadAll(f)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(b)).To(BeElementOf([]string{"helloworld", "worldhello"}))
			Expect(fs.Delete(fpk)).To(Succeed())
		})
	})
	Describe("Delete", func() {
		It("Should allow the caller to Delete the KeyFile twice", func() {
			fs := caesium.NewKFS(caesium.NewOS("testdata"))
			fpk := caesium.NewPK()
			_, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			fs.Release(fpk)
			Expect(fs.Delete(fpk)).To(Succeed())
			Expect(fs.Delete(fpk)).To(Succeed())
		})
	})
	Describe("Create", func() {
		It("Should allow for hundreds of concurrent writes to the KeyFile", func() {
			fs := caesium.NewKFS(caesium.NewOS("testdata"))
			fpk := caesium.NewPK()
			wg := sync.WaitGroup{}
			wg.Add(50)
			var expResBytes []byte
			for i := 0; i < 50; i++ {
				b := []byte("hello")
				expResBytes = append(expResBytes, b...)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					f, err := fs.Acquire(fpk)
					Expect(err).ToNot(HaveOccurred())
					_, err = f.Write(b)
					Expect(err).ToNot(HaveOccurred())
					fs.Release(fpk)
				}()
			}
			wg.Wait()
			f, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			_, err = f.Seek(0, 0)
			Expect(err).ToNot(HaveOccurred())
			defer fs.Release(fpk)
			b, err := ioutil.ReadAll(f)
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(Equal(expResBytes))
			Expect(fs.Delete(fpk)).To(Succeed())
		})
	})
})
