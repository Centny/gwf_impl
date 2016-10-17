package notifymgo

import (
	"github.com/Centny/gwf/netw/rc/plugin"
	"github.com/Centny/gwf/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//Indexes is the mongo findex field
var Indexes = map[string]mgo.Index{
	"message_oid": mgo.Index{
		Key: []string{"oid"},
	},
	"message_owner": mgo.Index{
		Key: []string{"owner"},
	},
	"message_type": mgo.Index{
		Key: []string{"type"},
	},
	"message_marked": mgo.Index{
		Key: []string{"marked"},
	},
	"message_count": mgo.Index{
		Key: []string{"count"},
	},
	"message_time": mgo.Index{
		Key: []string{"time"},
	},
}

//NotifyMgo is impl to NotifyDb by mongo
type NotifyMgo struct {
	Name  string
	C     func(name string) *mgo.Collection
	Count map[string]int
}

//NewNotifyMgo is NotifyMgo creator
func NewNotifyMgo(c func(name string) *mgo.Collection) *NotifyMgo {
	return &NotifyMgo{
		Name:  "rc_notify",
		C:     c,
		Count: map[string]int{},
	}
}

//AddMessage @see NotifyDb
func (n *NotifyMgo) AddMessage(m *plugin.Message) error {
	m.ID = bson.NewObjectId().Hex()
	m.Time = util.Now()
	return n.C(n.Name).Insert(m)
}

//RemoveMessage @see NotifyDb
func (n *NotifyMgo) RemoveMessage(id string) error {
	return n.C(n.Name).RemoveId(id)
}

//DoneMessage @see NotifyDb
func (n *NotifyMgo) DoneMessage(mid, key string) (msg *plugin.Message, err error) {
	msg = &plugin.Message{}
	_, err = n.C(n.Name).Find(bson.M{
		"_id": mid,
		"marked": bson.M{
			"$ne": key,
		},
	}).Apply(mgo.Change{
		Upsert:    false,
		ReturnNew: true,
		Update: bson.M{
			"$addToSet": bson.M{
				"marked": key,
			},
			"$inc": bson.M{
				"count": 1,
			},
		},
	}, msg)
	return
}

//RemoveCount @see NotifyDb
func (n *NotifyMgo) RemoveCount(mtype string) (count int, err error) {
	if val, ok := n.Count[mtype]; ok {
		return val, nil
	}
	return 1, nil
}

//ListMessage @see NotifyDb
func (n *NotifyMgo) ListMessage(m *plugin.Message) (ms []*plugin.Message, err error) {
	err = n.C(n.Name).Find(bson.M{"type": m.Type}).All(&ms)
	return
}
