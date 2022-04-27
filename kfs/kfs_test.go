package kfs_test

import (
	"cesium/kfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("KFS", func() {
	Describe("new", func() {
		It("Should wrap an existing file system without error", func() {
			Expect(func() { kfs.New("testdata") }).ToNot(Panic())
		})
	})
	Describe("Acquire and Release", func() {
		var (
			baseFS kfs.BaseFS
			fs     kfs.FS
		)
		BeforeEach(func() {
			baseFS = kfs.NewMem()
			fs = kfs.New("testdata", kfs.WithSuffix(".test"), kfs.WithFS(baseFS))
		})
		AfterEach(func() {
			Expect(fs.RemoveAll()).To(BeNil())
		})
		It("Should acquire and release a single file", func() {
			f, err := fs.Acquire(1)
			Expect(err).ToNot(HaveOccurred())
			Expect(f).ToNot(BeNil())
			fs.Release(1)
		})
		It("Should allow multiple goroutines to access the file ", func() {
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				_, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				fs.Release(1)
				wg.Done()
			}()
			go func() {
				_, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				fs.Release(1)
				wg.Done()
			}()
			wg.Wait()
		})
		It("Should allow multiple goroutines to write to the file", func() {
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				f, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				_, err = f.Write([]byte("hello"))
				Expect(err).ToNot(HaveOccurred())
				fs.Release(1)
			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				f, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				_, err = f.Write([]byte("world"))
				if err != nil {
					panic(err)
				}
				fs.Release(1)
			}()
			wg.Wait()
			f, err := baseFS.Open("testdata/1.test")
			Expect(err).ToNot(HaveOccurred())
			defer func() {
				defer GinkgoRecover()
				Expect(f.Close()).To(BeNil())
			}()
			b := make([]byte, 10)
			_, err = f.Read(b)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(b)).To(BeElementOf([]string{"helloworld", "worldhello"}))
		})
	})
})