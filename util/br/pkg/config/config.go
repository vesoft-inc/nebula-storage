package config

type BackupConfig struct {
	MetaAddrs    []string
	StorageAddrs []string
	SpaceNames   []string
	BackendUrl   string
	StorageUser  string
	MetaUser     string
}

type RestoreConfig struct {
	MetaAddrs      []string
	StorageAddrs   []string
	BackendUrl     string
	MetaUser       string
	StorageUser    string
	BackupName     string
	StorageDataDir string
	MetaDataDir    string
	StorageRootDir string
	MetaRootDir    string
}
