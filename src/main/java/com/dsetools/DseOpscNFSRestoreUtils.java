package com.dsetools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DseOpscNFSRestoreUtils {

    // Key string in Yaml config file
    static String CFG_KEY_CONTACT_POINT = "dse_contact_point";
    static String CFG_KEY_LOCAL_DOWNLOAD_HOME = "local_download_home";
    static String CFG_KEY_OPSC_NFS_BKUP_HOMEDIR = "nfs_backup_home";
    static String CFG_KEY_USE_SSL = "use_ssl";
    static String CFG_KEY_USER_AUTH = "user_auth";

    static String OPSC_NFS_OBJKEY_BASESTR = "snapshots";
    static String OPSC_NFS_OBJKEY_OPSC_MARKER_STR = "opscenter_adhoc";
    static String OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR = "sstables";

    static String CASSANDRA_SSTABLE_FILE_CODE = "mc";
    static int DOWNLOAD_THREAD_POOL_SIZE = 5;

    static String CMD_OPTION_HELP_SHORT = "h";
    static String CMD_OPTION_HELP_LONG = "help";
    static String CMD_OPTION_LIST_SHORT = "l";
    static String CMD_OPTION_LIST_LONG = "list";
    static String CMD_OPTION_LIST_ALL = "ALL";
    static String CMD_OPTION_LIST_DC = "DC";
    static String CMD_OPTION_LIST_ME = "ME";
    static String CMD_OPTION_CFG_SHORT = "c";
    static String CMD_OPTION_CFG_LONG = "config";
    static String CMD_OPTION_DOWNLOAD_SHORT = "d";
    static String CMD_OPTION_DOWNLOAD_LONG = "download";
    static String CMD_OPTION_KEYSPACE_SHORT = "k";
    static String CMD_OPTION_KEYSPACE_LONG = "keyspace";
    static String CMD_OPTION_TABLE_SHORT = "t";
    static String CMD_OPTION_TABLE_LONG = "table";
    static String CMD_OPTION_BACKUPTIME_SHORT = "obt";
    static String CMD_OPTION_BACKUPTIME_LONG = "opscBkupTime";
    static String CMD_OPTION_CLSDOWNDIR_SHORT = "cls";
    static String CMD_OPTION_CLSDOWNDIR_LONG = "clsDownDir";
    static String CMD_OPTION_NODIR_SHORT = "nds";
    static String CMD_OPTION_NODIR_LONG = "noDirStruct";
    static String CMD_OPTION_USER_SHORT = "u";
    static String CMD_OPTION_USER_LONG = "user";
    static String CMD_OPTION_PWD_SHORT = "p";
    static String CMD_OPTION_PWD_LONG = "password";


    static Properties LoadConfigFile(String configFilePath) {

        Properties configProps = null;

        try {
            InputStream inputStream = new FileInputStream(configFilePath);
            configProps = new Properties();
            configProps.load(inputStream);

            String dseContactPoint = configProps.getProperty(CFG_KEY_CONTACT_POINT);
            String localDownloadHome = configProps.getProperty(CFG_KEY_LOCAL_DOWNLOAD_HOME);
            String nfsBackupLocation = configProps.getProperty(CFG_KEY_OPSC_NFS_BKUP_HOMEDIR);

            String useSslStr = configProps.getProperty(CFG_KEY_USE_SSL);
            String userAuthStr = configProps.getProperty(CFG_KEY_USER_AUTH);


            if ( (dseContactPoint == null) || dseContactPoint.isEmpty() ) {
                System.out.println("ERROR: Empty value for configuration file parameter \"" + CFG_KEY_CONTACT_POINT + "\".");
            }

            if ( (localDownloadHome == null) || localDownloadHome.isEmpty() ) {
                System.out.println("ERROR: Empty value for configuration file parameter \"" + CFG_KEY_LOCAL_DOWNLOAD_HOME + "\".");
            }

            if ( (nfsBackupLocation == null) || nfsBackupLocation.isEmpty() ) {
                System.out.println("ERROR: Empty value for configuration file parameter \"" + CFG_KEY_OPSC_NFS_BKUP_HOMEDIR + "\".");
            }

            try {
                boolean useSsl = Boolean.parseBoolean(userAuthStr);
            }
            catch (NumberFormatException nfe) {
                System.out.println("ERROR: Incorrect value for configuration file parameter \"" + CFG_KEY_USE_SSL + "\".");
            }

            try {
                boolean userAuth = Boolean.parseBoolean(userAuthStr);
            }
            catch (NumberFormatException nfe) {
                System.out.println("ERROR: Incorrect value for configuration file parameter  \"" + CFG_KEY_USER_AUTH + "\".");
            }
        }
        catch (IOException ioe) {
            System.out.format("ERROR: failed to read/process configuration file (%s)\n.", configFilePath);
            ioe.printStackTrace();
        }

        return configProps;
    }
}