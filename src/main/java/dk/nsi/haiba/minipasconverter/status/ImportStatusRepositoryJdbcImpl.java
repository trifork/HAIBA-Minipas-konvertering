/**
 * The MIT License
 *
 * Original work sponsored and donated by National Board of e-Health (NSI), Denmark
 * (http://www.nsi.dk)
 *
 * Copyright (C) 2011 National Board of e-Health (NSI), Denmark (http://www.nsi.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package dk.nsi.haiba.minipasconverter.status;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import dk.nsi.haiba.minipasconverter.dao.impl.CommonDAO;

public class ImportStatusRepositoryJdbcImpl extends CommonDAO implements ImportStatusRepository {
    private static final Logger aLog = Logger.getLogger(ImportStatusRepositoryJdbcImpl.class);

    @Value("${max.days.between.runs}")
    private int maxDaysBetweenRuns;

    @Autowired
    private JdbcTemplate haibaJdbcTemplate;

    @Autowired
    private TimeSource timeSource;

    @Value("${jdbc.haibatableprefix:}")
    String tableprefix;

    public ImportStatusRepositoryJdbcImpl(String dialect) {
        super(dialect);
    }
    
    @Override
    @Transactional(value = "haibaTransactionManager")
    public void importStartedAt(DateTime startTime) {
        aLog.debug("Starting import");
        haibaJdbcTemplate.update("INSERT INTO " + tableprefix + "MedicinImporterStatus (StartTime) values (?)",
                startTime.toDate());
    }

    @Override
    @Transactional(value = "haibaTransactionManager")
    public void importEndedWithSuccess(DateTime endTime) {
        aLog.debug("Import ended with success");
        importEndedAt(endTime, ImportStatus.Outcome.SUCCESS, null);
    }

    @Override
    @Transactional(value = "haibaTransactionManager")
    public void importEndedWithFailure(DateTime endTime, String errorMessage) {
        aLog.debug("Import ended with failure");
        if (errorMessage != null && errorMessage.length() > 200) {
            errorMessage = errorMessage.substring(0, 200); // truncate to match db layout
        }
        importEndedAt(endTime, ImportStatus.Outcome.FAILURE, errorMessage);
    }

    private void importEndedAt(DateTime endTime, ImportStatus.Outcome outcome, String errorMessage) {
        String sql = null;
        if (MYSQL.equals(getDialect())) {
            sql = "SELECT Id from MedicinImporterStatus ORDER BY StartTime DESC LIMIT 1";
        } else {
            // MSSQL
            sql = "SELECT Top 1 Id from " + tableprefix + "MedicinImporterStatus ORDER BY StartTime DESC";
        }

        Long newestOpenId;
        try {
            newestOpenId = haibaJdbcTemplate.queryForLong(sql);
        } catch (EmptyResultDataAccessException e) {
            aLog.debug("it seems we do not have any open statuses, let's not update");
            return;
        }

        haibaJdbcTemplate.update("UPDATE " + tableprefix
                + "MedicinImporterStatus SET EndTime=?, Outcome=?, ErrorMessage=? WHERE Id=?", endTime.toDate(),
                outcome.toString(), errorMessage, newestOpenId);
    }

    @Override
    public ImportStatus getLatestStatus() {
        String sql = null;
        if (MYSQL.equals(getDialect())) {
            sql = "SELECT * from MedicinImporterStatus ORDER BY StartTime DESC LIMIT 1";
        } else {
            // MSSQL
            sql = "SELECT Top 1 * from " + tableprefix + "MedicinImporterStatus ORDER BY StartTime DESC";
        }

        try {
            return haibaJdbcTemplate.queryForObject(sql, new ImportStatusRowMapper());
        } catch (EmptyResultDataAccessException ignored) {
            // that's not a problem, we just don't have any statuses
            return null;
        }
    }

    @Override
    /**
     *        {maxDaysBetweenRuns}
     *     <-----------------------------
     *
     * ____'________|________'___________|___________
     *           lastRun                now
     *     ^                 ^
     *   !overdue         overdue
     */
    public boolean isOverdue() {
        ImportStatus latestStatus = getLatestStatus();
        if (latestStatus == null) {
            // we're not overdue if we have never run
            return false;
        }

        DateTime lastRun = latestStatus.getStartTime();
        DateTime now = timeSource.now();
        return (now.minusDays(maxDaysBetweenRuns).isAfter(lastRun));
    }

    private class ImportStatusRowMapper implements RowMapper<ImportStatus> {

        @Override
        public ImportStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
            ImportStatus status = new ImportStatus();
            status.setStartTime(new DateTime(rs.getTimestamp("StartTime")));
            Timestamp endTime = rs.getTimestamp("EndTime");
            if (endTime != null) {
                status.setEndTime(new DateTime(endTime));
            }
            String dbOutcome = rs.getString("Outcome");
            if (dbOutcome != null) {
                status.setOutcome(ImportStatus.Outcome.valueOf(dbOutcome));
            }
            status.setErrorMessage(rs.getString("ErrorMessage"));

            return status;
        }
    }

    public boolean isHAIBADBAlive() {
        String sql = null;
        if (MYSQL.equals(getDialect())) {
            sql = "SELECT insertrow_id from region_medicin LIMIT 1";
        } else {
            // MSSQL
            sql = "SELECT Top 1 insertrow_id from " + tableprefix + "region_medicin";
        }

        try {
            haibaJdbcTemplate.queryForObject(sql, Long.class);
        } catch (EmptyResultDataAccessException e) {
            // no data was found, but table exists, so everything is ok
        } catch (Exception someError) {
            aLog.debug("isHAIBADBAlive", someError);
            return false;
        }
        return true;
    }
}
