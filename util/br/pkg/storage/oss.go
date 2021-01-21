package storage

import (
	"fmt"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"
)

type OSSBackedStore struct {
	url           string
	log           *zap.Logger
	backupName    string
	maxConcurrent string
}

func NewOSSBackendStore(url string, log *zap.Logger, maxConcurrent int) *OSSBackedStore {
	return &OSSBackedStore{url: url, log: log, maxConcurrent: strconv.Itoa(maxConcurrent)}
}

func (s *OSSBackedStore) SetBackupName(name string) {
	s.backupName = name
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
}

func (s *OSSBackedStore) BackupPreCommand() []string {
	return nil
}

func (s *OSSBackedStore) BackupStorageCommand(src string, host string, spaceID string) string {
	storageDir := s.url + "/" + "storage/" + host + "/" + spaceID + "/"
	return "ossutil cp -r " + src + " " + storageDir + " -j " + s.maxConcurrent
}

func (s OSSBackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta/"
	return "ossutil cp -r " + filepath.Dir(src[0]) + " " + metaDir + " -j " + s.maxConcurrent
}

func (s OSSBackedStore) BackupMetaFileCommand(src string) []string {
	return []string{"ossutil", "cp", "-r", src, s.url + "/", "-j", s.maxConcurrent}
}

func (s OSSBackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	return []string{"ossutil", "cp", "-r", s.url + "/" + file, dst, "-j", s.maxConcurrent}
}

func (s OSSBackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	var sstFiles []string
	for _, f := range src {
		file := dst + "/" + f
		sstFiles = append(sstFiles, file)
	}
	return fmt.Sprintf("ossutil cp -r %s %s -j %s ", metaDir, dst, s.maxConcurrent), sstFiles
}
func (s OSSBackedStore) RestoreStorageCommand(host string, spaceID []string, dst string) string {
	storageDir := s.url + "/storage/" + host + "/"

	return fmt.Sprintf("ossutil cp -r %s %s -j %s ", storageDir, dst, s.maxConcurrent)
}
func (s OSSBackedStore) RestoreMetaPreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s OSSBackedStore) RestoreStoragePreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s OSSBackedStore) URI() string {
	return s.url
}

func (s OSSBackedStore) CheckCommand() string {
	return "ossutil ls " + s.url
}
