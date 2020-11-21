package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/restore"
	"go.uber.org/zap"
)

func NewRestoreCMD() *cobra.Command {
	restoreCmd := &cobra.Command{
		Use:          "restore",
		Short:        "restore Nebula Graph Database",
		SilenceUsage: true,
	}

	restoreCmd.AddCommand(newFullRestoreCmd())
	restoreCmd.PersistentFlags().StringArrayVar(&restoreConfig.MetaAddrs, "meta", nil, "meta server url")
	restoreCmd.MarkPersistentFlagRequired("meta")
	restoreCmd.PersistentFlags().StringArrayVar(&restoreConfig.StorageAddrs, "storage", nil, "storage server url")
	restoreCmd.MarkPersistentFlagRequired("storage")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackendUrl, "backend", "", "backend url")
	restoreCmd.MarkPersistentFlagRequired("backend")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.StorageUser, "storageuser", "", "storage server user")
	restoreCmd.MarkPersistentFlagRequired("storageuser")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.MetaUser, "metauser", "", "meta server user")
	restoreCmd.MarkPersistentFlagRequired("metauser")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.BackupName, "backupname", "", "backup name")
	restoreCmd.MarkPersistentFlagRequired("backupname")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.StorageDataDir, "sdir", "", "storage data dir")
	restoreCmd.MarkPersistentFlagRequired("sdir")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.MetaDataDir, "mdir", "", "meta data dir")
	restoreCmd.MarkPersistentFlagRequired("mdir")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.StorageRootDir, "srdir", "", "storage root dir")
	restoreCmd.MarkPersistentFlagRequired("srdir")
	restoreCmd.PersistentFlags().StringVar(&restoreConfig.MetaRootDir, "mrdir", "", "meta root dir")
	restoreCmd.MarkPersistentFlagRequired("srdir")

	return restoreCmd
}

func newFullRestoreCmd() *cobra.Command {
	fullRestoreCmd := &cobra.Command{
		Use:   "full",
		Short: "full restore Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()
			defer logger.Sync() // flushes buffer, if any
			err := checkSSH(restoreConfig.StorageAddrs, restoreConfig.StorageUser, logger)
			if err != nil {
				return err
			}

			err = checkSSH(restoreConfig.MetaAddrs, restoreConfig.MetaUser, logger)
			if err != nil {
				return err
			}

			if !checkPathAbs(restoreConfig.StorageDataDir) {
				logger.Error("StorageDataDir must be an absolute path.", zap.String("dir", restoreConfig.StorageDataDir))
				return fmt.Errorf("StorageDataDir must be an absolute path")
			}

			if !checkPathAbs(restoreConfig.MetaDataDir) {
				logger.Error("MetaDataDir must be an absolute path.", zap.String("dir", restoreConfig.MetaDataDir))
				return fmt.Errorf("MetaDataDir must be an absolute path")
			}

			if !checkPathAbs(restoreConfig.StorageRootDir) {
				logger.Error("StorageRootDir must be an absolute path.", zap.String("dir", restoreConfig.StorageRootDir))
				return fmt.Errorf("StorageRootDir must be an absolute path")
			}

			if !checkPathAbs(restoreConfig.MetaRootDir) {
				logger.Error("MetaRootDir must be an absolute path.", zap.String("dir", restoreConfig.MetaRootDir))
				return fmt.Errorf("MetaRootDir must be an absolute path")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any

			r := restore.NewRestore(restoreConfig, logger)
			err := r.RestoreCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullRestoreCmd
}
