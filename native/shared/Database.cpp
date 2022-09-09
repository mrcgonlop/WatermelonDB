#include "Database.h"

namespace watermelondb {

using platform::consoleError;
using platform::consoleLog;

Database::Database(jsi::Runtime *runtime, std::string path, bool usesExclusiveLocking) : runtime_(runtime), mutex_() {
    db_ = std::make_unique<SqliteDb>(path);

    std::string initSql = "";

    // FIXME: On Android, Watermelon often errors out on large batches with an IO error, because it
    // can't find a temp store... I tried setting sqlite3_temp_directory to /tmp/something, but that
    // didn't work. Setting temp_store to memory seems to fix the issue, but causes a significant
    // slowdown, at least on iOS (not confirmed on Android). Worth investigating if the slowdown is
    // also present on Android, and if so, investigate the root cause. Perhaps we need to set the temp
    // directory by interacting with JNI and finding a path within the app's sandbox?
    #ifdef ANDROID
    initSql += "pragma temp_store = memory;";
    #endif

    initSql += "pragma journal_mode = WAL;";

    // set timeout before SQLITE_BUSY error is returned
    initSql += "pragma busy_timeout = 5000;";

    #ifdef ANDROID
    // NOTE: This was added in an attempt to fix mysterious `database disk image is malformed` issue when using
    // headless JS services
    // NOTE: This slows things down
    initSql += "pragma synchronous = FULL;";
    #endif
    if (usesExclusiveLocking) {
        // this seems to fix the headless JS service issue but breaks if you have multiple readers
        initSql += "pragma locking_mode = EXCLUSIVE;";
    }

    executeMultiple(initSql);
}

void Database::destroy() {
    const std::lock_guard<std::mutex> lock(mutex_);

    if (isDestroyed_) {
        return;
    }
    isDestroyed_ = true;
    for (auto const &cachedStatement : cachedStatements_) {
        sqlite3_stmt *statement = cachedStatement.second;
        sqlite3_finalize(statement);
    }
    cachedStatements_ = {};
    db_->destroy();
}

Database::~Database() {
    destroy();
}

bool Database::isCached(std::string cacheKey) {
    return cachedRecords_.find(cacheKey) != cachedRecords_.end();
}
void Database::markAsCached(std::string cacheKey) {
    cachedRecords_.insert(cacheKey);
}
void Database::removeFromCache(std::string cacheKey) {
    cachedRecords_.erase(cacheKey);
}

void Database::unsafeResetDatabase(jsi::String &schema, int schemaVersion) {
    auto &rt = getRt();
    const std::lock_guard<std::mutex> lock(mutex_);

    // TODO: in non-memory mode, just delete the DB files
    // NOTE: As of iOS 14, selecting tables from sqlite_master and deleting them does not work
    // They seem to be enabling "defensive" config. So we use another obscure method to clear the database
    // https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigresetdatabase

    if (sqlite3_db_config(db_->sqlite, SQLITE_DBCONFIG_RESET_DATABASE, 1, 0) != SQLITE_OK) {
        throw jsi::JSError(rt, "Failed to enable reset database mode");
    }
    // NOTE: We can't VACUUM in a transaction
    executeMultiple("vacuum");

    if (sqlite3_db_config(db_->sqlite, SQLITE_DBCONFIG_RESET_DATABASE, 0, 0) != SQLITE_OK) {
        throw jsi::JSError(rt, "Failed to disable reset database mode");
    }

    beginTransaction();
    try {
        cachedRecords_ = {};

        // Reinitialize schema
        executeMultiple(schema.utf8(rt));
        setUserVersion(schemaVersion);

        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }
}

void Database::migrate(jsi::String &migrationSql, int fromVersion, int toVersion) {
    auto &rt = getRt();
    const std::lock_guard<std::mutex> lock(mutex_);

    beginTransaction();
    try {
        assert(getUserVersion() == fromVersion && "Incompatible migration set");

        executeMultiple(migrationSql.utf8(rt));
        setUserVersion(toVersion);

        commit();
    } catch (const std::exception &ex) {
        rollback();
        throw;
    }
}

void Database::loadOrSaveDb(jsi::String &zFilename, int isSave) {

    auto &rt = getRt();
    sqlite3 *pInMemory = db_->sqlite;

    int rc;                   /* Function return code */
    sqlite3 *pFile;           /* Database connection opened on zFilename */
    sqlite3_backup *pBackup;  /* Backup object used to copy data */
    sqlite3 *pTo;             /* Database to copy to (pFile or pInMemory) */
    sqlite3 *pFrom;           /* Database to copy from (pFile or pInMemory) */

    /* Open the database file identified by zFilename. Exit early if this fails
     ** for any reason. */
    rc = sqlite3_open(zFilename.utf8(rt).c_str(), &pFile);
    if( rc==SQLITE_OK ){

        /* If this is a 'load' operation (isSave==0), then data is copied
         ** from the database file just opened to database pInMemory.
         ** Otherwise, if this is a 'save' operation (isSave==1), then data
         ** is copied from pInMemory to pFile.  Set the variables pFrom and
         ** pTo accordingly. */
        pFrom = (isSave ? pInMemory : pFile);
        pTo   = (isSave ? pFile     : pInMemory);

        /* Set up the backup procedure to copy from the "main" database of
         ** connection pFile to the main database of connection pInMemory.
         ** If something goes wrong, pBackup will be set to NULL and an error
         ** code and  message left in connection pTo.
         **
         ** If the backup object is successfully created, call backup_step()
         ** to copy data from pFile to pInMemory. Then call backup_finish()
         ** to release resources associated with the pBackup object.  If an
         ** error occurred, then  an error code and message will be left in
         ** connection pTo. If no error occurred, then the error code belonging
         ** to pTo is set to SQLITE_OK.
         */
        pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main");
        if( pBackup ){
            (void)sqlite3_backup_step(pBackup, -1);
            (void)sqlite3_backup_finish(pBackup);
        }
        rc = sqlite3_errcode(pTo);
    }

    /* Close the database connection opened on database file zFilename
     ** and return the result of this function. */
    (void)sqlite3_close(pFile);
//    return rc;
}

} // namespace watermelondb
