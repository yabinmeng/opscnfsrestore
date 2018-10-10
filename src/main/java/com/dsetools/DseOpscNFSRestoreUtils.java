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
    static String CFG_KEY_IP_MATCHING_NIC = "ip_matching_nic";
    static String CFG_KEY_USE_SSL = "use_ssl";
    static String CFG_KEY_USER_AUTH = "user_auth";
    static String CFG_KEY_FILE_SIZE_CHK = "file_size_chk";
    static String CFG_KEY_BKUP_LIST_SRC_FILENAME = "bkuplist_src_file_name";

    static String JAVA_SSL_TRUSTSTORE_PROP = "javax.net.ssl.trustStore";
    static String JAVA_SSL_TRUSTSTORE_PASS_PROP = "javax.net.ssl.trustStorePassword";

    static String OPSC_NFS_OBJKEY_BASESTR = "snapshots";
    static String OPSC_NFS_OBJKEY_OPSC_MARKER_STR = "opscenter";
    static String OPSC_NFS_OBJKEY_OPSC_ADHOC_MARKER_STR = OPSC_NFS_OBJKEY_OPSC_MARKER_STR + "_adhoc";
    static String OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR = "sstables";
    static String OPSC_BKUP_METADATA_FILE = "backup.json";

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

    static String CMD_OPTION_DEBUG_SHORT = "dbg";
    static String CMD_OPTION_DEBUG_LONG = "debug";


    static Properties LoadConfigFile(String configFilePath) {

        Properties configProps = null;

        try {
            InputStream inputStream = new FileInputStream(configFilePath);
            configProps = new Properties();
            configProps.load(inputStream);

            String dseContactPoint = configProps.getProperty(CFG_KEY_CONTACT_POINT);
            String localDownloadHome = configProps.getProperty(CFG_KEY_LOCAL_DOWNLOAD_HOME);
            String nfsBackupLocation = configProps.getProperty(CFG_KEY_OPSC_NFS_BKUP_HOMEDIR);
            String ipMatchingNic = configProps.getProperty(CFG_KEY_IP_MATCHING_NIC);
            String useSslStr = configProps.getProperty(CFG_KEY_USE_SSL);
            String userAuthStr = configProps.getProperty(CFG_KEY_USER_AUTH);
            String fileSizeMonStr = configProps.getProperty(CFG_KEY_FILE_SIZE_CHK);
            String bkupListSrcFile = configProps.getProperty(CFG_KEY_BKUP_LIST_SRC_FILENAME);

            // An active DSE contact point is not a must for all cases. Log a warning message if not specified.
            if ( (dseContactPoint == null) || dseContactPoint.isEmpty() ) {
                System.out.println("WARN: Empty value for configuration file parameter \"" + CFG_KEY_CONTACT_POINT + "\".");
            }

            // Local download home directory is a must. If not specified, error out
            if ( (localDownloadHome == null) || localDownloadHome.isEmpty() ) {
                System.out.println("ERROR: Empty value for configuration file parameter \"" + CFG_KEY_LOCAL_DOWNLOAD_HOME + "\".");
                configProps = null;
            }

            // NFS backup location is a must. If not specified, error out
            if ( (nfsBackupLocation == null) || nfsBackupLocation.isEmpty() ) {
                System.out.println("ERROR: Empty value for configuration file parameter \"" + CFG_KEY_OPSC_NFS_BKUP_HOMEDIR + "\".");
                configProps = null;
            }

            // "ip_matching_nic" is not a must for all cases. Log a warning message if not specified.
            if ( (ipMatchingNic == null) || ipMatchingNic.isEmpty() ) {
                System.out.println("WARN: Empty value for configuration file parameter \"" + CFG_KEY_IP_MATCHING_NIC + "\".");
            }

            // When "use_ssl" is specified, it must be a valid type that can convert to boolean. Otherwise, error out.
            if ( (useSslStr != null) && (!useSslStr.isEmpty()) ) {
                try {
                    Boolean.parseBoolean(useSslStr);
                }
                catch (NumberFormatException nfe) {
                    System.out.println("ERROR: Incorrect value for configuration file parameter \"" + CFG_KEY_USE_SSL + "\".");
                    configProps = null;
                }
            }

            // When "user_auth" is specified, it must be a valid type that can convert to boolean. Otherwise, error out.
            if ( (userAuthStr != null) && (!userAuthStr.isEmpty()) ) {
                try {
                    Boolean.parseBoolean(userAuthStr);
                }
                catch (NumberFormatException nfe) {
                    System.out.println("ERROR: Incorrect value for configuration file parameter  \"" + CFG_KEY_USER_AUTH + "\".");
                    configProps = null;
                }
            }

            // When "file_size_mon" is specified, it must be a valid type that can convert to boolean. Otherwise, error out.
            if ( (fileSizeMonStr != null) && (!fileSizeMonStr.isEmpty()) ) {
                try {
                    Boolean.parseBoolean(fileSizeMonStr);
                }
                catch (NumberFormatException nfe) {
                    System.out.println("ERROR: Incorrect value for configuration file parameter  \"" + CFG_KEY_FILE_SIZE_CHK + "\".");
                    configProps = null;
                }
            }

            // The backup list source file specifies the original backup folder structure for each backed-up file.
            // Log a warning message if this property setting not specified.
            if ( (bkupListSrcFile == null) || bkupListSrcFile.isEmpty() ) {
                System.out.println("WARN: Empty value for configuration file parameter \"" + CFG_KEY_BKUP_LIST_SRC_FILENAME + "\".");
            }
        }
        catch (IOException ioe) {
            System.out.format("ERROR: failed to read/process configuration file (%s)\n.", configFilePath);
            ioe.printStackTrace();
        }

        return configProps;
    }
}