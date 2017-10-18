package notifymgo

import (
	"fmt"
	"testing"
	"time"

	"github.com/Centny/gwf/netw/rc/plugin"
	"github.com/Centny/gwf/netw/rc/rctest"
	mongoc "gopkg.in/mongoc.v1"
)

func TestNotifyMgo(t *testing.T) {
	mongoc.InitShared("cny:123@loc.w:27017/test", "test")
	mongoc.SharedC("rc_notify").RemoveAll(nil)
	plugin.ShowLog = 1
	var err error
	var rct *rctest.RCTest
	var mdb *NotifyMgo
	var srv *plugin.NotifySrv
	var client *plugin.NotifyClient
	var received = 0

	//
	//test normal post
	{
		received = 0
		rct = rctest.NewRCTest_j2(":9332")
		mdb = NewNotifyMgo(mongoc.SharedC)
		srv = plugin.NewNotifySrv(mdb)
		srv.Hand(rct.L)
		srv.Start()
		//
		client = plugin.NewNotifyClient(plugin.NotifyHandlerF(func(n *plugin.NotifyClient, m *plugin.Message) error {
			if len(m.ID) < 1 {
				panic("xxx")
			}
			if m.Type == "testing" || m.Type == "xx" {
				var err = n.Mark(m.ID, "testing")
				if err != nil {
					t.Error(err)
					return err
				}
				received++
				return err
			}
			return nil
		}))
		client.SetRunner(rct.R, rct.Rmh)
		//
		err = client.Monitor("testing,xx", 10)
		if err != nil {
			t.Error(err)
			return
		}
		for i := 0; i < 10; i++ {
			err = srv.PostMessage(&plugin.Message{
				Type: "testing",
			})
			if err != nil {
				t.Error(err)
				return
			}
			err = srv.PostMessage(&plugin.Message{
				Type: "xx",
			})
			if err != nil {
				t.Error(err)
				return
			}
		}
		for x := 0; x < 10 && received < 20; x++ {
			time.Sleep(100 * time.Millisecond)
		}
		if received != 20 {
			t.Error("not received")
			return
		}
		if cc, _ := mongoc.SharedC(mdb.Name).Count(nil, 0, 0); cc > 0 {
			t.Error("error")
			return
		}
		srv.Stop()
		rct.R.Stop()
		rct.L.Close()
		rct.L.Wait()
		fmt.Printf("\n\n\n")
	}
}
