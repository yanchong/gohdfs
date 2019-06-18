package hdfs

import (
	"github.com/golang/protobuf/proto"
	hdfs "github.com/yanchong/gohdfs/internal/protocol/hadoop_hdfs"
	"github.com/yanchong/gohdfs/internal/rpc"
	"os"
	"path"
)

// ReadDir reads the directory named by dirname and returns a list of sorted
// directory entries.
//
// The os.FileInfo values returned will not have block location attached to
// the struct returned by Sys().
//func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
//	f, err := c.Open(dirname)
//	if err != nil {
//		return nil, err
//	}
//
//	return f.Readdir(0)
//}

// ReadDir reads the directory named by dirname and returns a list of sorted
// directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.getDirList(dirname, "", 0)
}

func (c *Client) getDirList(dirname string, after string, max int) ([]os.FileInfo, error) {
	var res []os.FileInfo
	last := after
	for max <= 0 || len(res) < max {
		partial, remaining, err := c.getPartialDirList(dirname, last)
		if err != nil {
			return nil, err
		}

		res = append(res, partial...)
		if remaining == 0 {
			break
		} else if len(partial) > 0 {
			last = partial[len(partial)-1].Name()
		}
	}

	if max > 0 && len(res) > max {
		res = res[:max]
	}

	return res, nil
}

func (c *Client) getPartialDirList(dirname string, after string) ([]os.FileInfo, int, error) {
	dirname = path.Clean(dirname)

	req := &hdfs.GetListingRequestProto{
		Src:          proto.String(dirname),
		StartAfter:   []byte(after),
		NeedLocation: proto.Bool(true),
	}
	resp := &hdfs.GetListingResponseProto{}

	err := c.namenode.Execute("getListing", req, resp)
	if err != nil {
		if _, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(err)
		}

		return nil, 0, &os.PathError{"readdir", dirname, err}
	} else if resp.GetDirList() == nil {
		return nil, 0, &os.PathError{"readdir", dirname, os.ErrNotExist}
	}

	list := resp.GetDirList().GetPartialListing()
	res := make([]os.FileInfo, 0, len(list))
	for _, status := range list {
		res = append(res, newFileInfo(status, ""))
	}

	remaining := int(resp.GetDirList().GetRemainingEntries())
	return res, remaining, nil
}
