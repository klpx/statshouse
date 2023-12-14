package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/vkcom/statshouse/internal/sqlite"
)

type args struct {
	path          string
	checkDbOffset bool
}

func checkDbOffset(path string) {

	eng, err := sqlite.OpenRO(sqlite.Options{
		Path:                   path,
		MaxROConn:              2,
		CacheMaxSizePerConnect: 10,
	})
	if err != nil {
		return
		panic(err)
	}
	defer eng.Close(context.Background())
	var offset int64
	err = eng.View(context.Background(), "check_offset", func(conn sqlite.Conn) error {
		rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
		if rows.Error() != nil {
			return rows.Error()
		}
		if rows.Next() {
			offset, _ = rows.ColumnInt64(0)
			return nil
		}
		return fmt.Errorf("empty?")
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("__binlog_offset:", offset)
}

type wal struct {
	origPath string
	path     string
	pgSize   int32
	chsq     int32
	body     []byte
	splitted [][]byte
}

func parseWalHdr(path string, body []byte) *wal {
	pgSize := int32(binary.BigEndian.Uint32(body[8:12]))
	chsq := int32(binary.BigEndian.Uint32(body[12:16]))
	fmt.Println("CHSQ", chsq)
	return &wal{
		origPath: path_bk_toOrig(path),
		path:     path,
		pgSize:   pgSize,
		chsq:     chsq,
		body:     body,
	}
}

func splitByCommit(pgSize int32, bodyWithoutHdr []byte) [][]byte {
	var cur []byte
	var res [][]byte
	b := bodyWithoutHdr
	fmt.Println("split")
	ix := 0
	for len(b) > 0 {
		frameHdr := b[:24]
		//if binary.BigEndian.Uint32(frameHdr[:4]) == 1 {
		//	fmt.Println("PAGE:", binary.BigEndian.Uint32(frameHdr[:4]), "IX:", ix, "COMMIT", len(res))
		//}
		ix++
		frame := b[:24+pgSize]
		b = b[24+pgSize:]
		cur = append(cur, frame...)
		dbSize := binary.BigEndian.Uint32(frameHdr[4:8])
		//fmt.Println(frameHdr[8:16])
		if dbSize != 0 {
			res = append(res, cur)
			cur = nil
		}
	}
	return res
}

var bkpSiffux = "_bkp"

func BYTESWAP32(x uint32) uint32 {
	return (((x) & 0x000000FF) << 24) + (((x) & 0x0000FF00) << 8) + (((x) & 0x00FF0000) >> 8) + (((x) & 0xFF000000) >> 24)
}
func walChecksumBytes(native bool, data []byte, initA, initB uint32) (uint32, uint32) {
	s1 := initA
	s2 := initB
	if !native {
		for i := 0; i < len(data); i += 2 {
			s1 += BYTESWAP32(uint32(data[i])) + s2
			s2 += BYTESWAP32(uint32(data[i+1])) + s1
		}
	} else {
		for i := 0; i < len(data); i += 2 {
			s1 += uint32(data[i]) + s2
			s2 += uint32(data[i+1]) + s1
		}
	}
	return s1, s2
}

func path_bk_toOrig(p string) string {
	return strings.TrimSuffix(p, bkpSiffux)
}

/*
777288
779916
*/
func openAndInitWal(w *wal, body []byte) *os.File {
	f, err := os.Create(w.origPath)
	if err != nil {
		if os.IsNotExist(err) {
			f, err = os.Create(w.origPath)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	if stat.Size() == 0 {
		//fmt.Println("init hdr")
		_, err := f.Write(body)
		if err != nil {
			panic(err)
		}
	}
	return f
}

func work(pathDb string, wals []*wal) {
	for _, wal := range wals {
		wal.splitted = splitByCommit(wal.pgSize, wal.body[32:])
		fmt.Println("LEN:", len(wal.splitted))
	}
	fmt.Println("WITHOUT WALS")
	check(pathDb)
	for _, w := range wals {
		f := openAndInitWal(w, w.body)
		_ = f.Close()
	}
	fmt.Println("WITH WALS")
	check(pathDb)
	for walI, w := range wals {
		fmt.Println("WAL:", walI)
		for i, _ := range w.splitted {
			for j := 0; j < walI; j++ {
				f := openAndInitWal(wals[j], wals[j].body)
				_ = f.Close()
			}
			f := openAndInitWal(w, w.body[:32])
			for j := 0; j <= i; j++ {
				_, err := f.Write(w.splitted[j])
				if err != nil {
					panic(err)
				}
			}
			_ = f.Close()
			check(pathDb)
		}
	}
}

func check(path string) {
	_ = os.Remove(path + "-shm")
	checkDbOffset(path)
	_ = os.Remove(path + "-shm")
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-wal2")
}

func main() {
	pathP := flag.String("path", "/Users/e.martyn/exper/db", "")
	flag.Parse()
	path := *pathP
	shm := path + "-shm"
	_ = os.Remove(shm)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-wal2")

	walP := path + "-wal" + bkpSiffux
	wal2P := path + "-wal2" + bkpSiffux
	fmt.Println(walP)
	fmt.Println(wal2P)

	walF, err := os.Open(walP)
	hasWal := err == nil
	wal2F, err := os.Open(wal2P)
	hasWal2 := err == nil
	var wals []*wal
	if hasWal {
		b, err := io.ReadAll(walF)
		if err != nil {
			panic(err)
		}
		wals = append(wals, parseWalHdr(walP, b))
	}
	if hasWal2 {
		b, err := io.ReadAll(wal2F)
		if err != nil {
			panic(err)
		}
		wals = append(wals, parseWalHdr(wal2P, b))
	}
	sort.Slice(wals, func(i, j int) bool {
		return wals[i].chsq < wals[j].chsq
	})
	for _, w := range wals {
		fmt.Println(w.origPath)
		fmt.Println(w.path)
		fmt.Println(w.pgSize)
		fmt.Println(w.chsq)
	}
	work(path, wals)
}
