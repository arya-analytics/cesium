package kfs_test

import (
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("sync", func() {
	It("Should sync the contents of the file to the file system every interval", func() {
		fs := kfs.New[int]("testdata", kfs.WithExtensionConfig(".test"), kfs.WithFS(kfs.NewMem()))
		defer Expect(fs.RemoveAll()).To(Succeed())
		_, err := fs.Acquire(1)
		Expect(err).To(BeNil())
		_, err = fs.Acquire(2)
		Expect(err).To(BeNil())
		_, err = fs.Acquire(3)
		Expect(err).To(BeNil())
		fs.Release(1)
		fs.Release(2)
		fs.Release(3)
		time.Sleep(5 * time.Millisecond)
		Expect(fs.Files()[1].Age() > 5*time.Millisecond).To(BeTrue())
		s := shut.New()
		sync := &kfs.Sync[int]{
			FS:       fs,
			Interval: 2 * time.Millisecond,
			MaxAge:   2 * time.Millisecond,
			Shutter:  s,
		}
		errs := sync.Start()
		go func() {
			defer GinkgoRecover()
			Expect(<-errs).ToNot(HaveOccurred())
		}()
		time.Sleep(6 * time.Millisecond)
		fOne := fs.Files()[1]
		Expect(fOne.Age() < 7*time.Millisecond).To(BeTrue())
		Expect(s.Shutdown()).To(Succeed())
	})
	It("Should sync the contents of all of the files on shutdown", func() {
		fs := kfs.New[int]("testdata", kfs.WithExtensionConfig(".test"), kfs.WithFS(kfs.NewMem()))
		defer Expect(fs.RemoveAll()).To(Succeed())
		_, err := fs.Acquire(1)
		Expect(err).To(BeNil())
		_, err = fs.Acquire(2)
		Expect(err).To(BeNil())
		_, err = fs.Acquire(3)
		Expect(err).To(BeNil())
		fs.Release(1)
		fs.Release(2)
		fs.Release(3)
		time.Sleep(5 * time.Millisecond)
		Expect(fs.Files()[1].Age() > 5*time.Millisecond).To(BeTrue())
		shutter := shut.New()
		sync := &kfs.Sync[int]{
			FS:       fs,
			Interval: 5 * time.Millisecond,
			MaxAge:   2 * time.Millisecond,
			Shutter:  shutter,
		}
		errs := sync.Start()
		go func() {
			defer GinkgoRecover()
			Expect(<-errs).ToNot(HaveOccurred())
		}()
		time.Sleep(15 * time.Millisecond)
		Expect(shutter.Shutdown()).To(Succeed())
		fOne := fs.Files()[1]
		Expect(fOne.Age() < 3*time.Millisecond).To(BeTrue())
	})
})
