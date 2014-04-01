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
package dk.nsi.haiba.minipasconverter.executor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO.MinipasSyncStructure;
import dk.nsi.haiba.minipasconverter.model.MinipasTADM;
import dk.nsi.haiba.minipasconverter.model.MinipasTDIAG;
import dk.nsi.haiba.minipasconverter.model.MinipasTSKSUBE_OPR;
import dk.nsi.haiba.minipasconverter.status.CurrentImportProgress;
import dk.nsi.haiba.minipasconverter.status.ImportStatusRepository;

public class MinipasPreprocessor {
    private static Logger aLog = Logger.getLogger(MinipasPreprocessor.class);

    @Value("${minipas.batchsize:100}")
    int batchSize;

    @Value("${minipas.noofretries:2}")
    int noOfRetries;

    @Value("${minipas.waitperiodminutes:30}")
    int waitPeriodMinutes;

    @Value("${minipas.yearstotal:6}")
    int yearstotal;

    @Autowired
    MinipasDAO minipasDao;

    @Autowired
    MinipasHAIBADAO haibaDao;

    @Autowired
    ImportStatusRepository statusRepo;

    @Autowired
    CurrentImportProgress currentImportProgress;

    private boolean manualOverride;
    private boolean running;

    @Scheduled(cron = "${cron.import.job}")
    public void run() {
        if (!isManualOverride()) {
            aLog.debug("Running Importer: " + new Date().toString());
            doProcess(false);
        } else {
            aLog.debug("Importer must be started manually");
        }
    }

    public synchronized void doProcess(boolean manual) {
        aLog.info("Started processing, manual=" + manual);
        if (!running) {
            statusRepo.importStartedAt(new DateTime());
            running = true;
            currentImportProgress.reset();
            try {
                int retries = noOfRetries;
                boolean doRetry = true;
                while (doRetry) {
                    long lastReturnCodeElseNegativeOne = minipasDao.lastReturnCodeElseNegativeOne();
                    if (minipasError(lastReturnCodeElseNegativeOne)) {
                        String status = "Aborting process, last V_RETURNCODE from MINIPAS is "
                                + lastReturnCodeElseNegativeOne;
                        currentImportProgress.addStatusLine(status);
                        aLog.warn(status);
                        doRetry = false;
                        statusRepo.importEndedWithFailure(new DateTime(), status);
                    } else if (minipasOk(lastReturnCodeElseNegativeOne)) {
                        doImport();
                        doRetry = false;
                        statusRepo.importEndedWithSuccess(new DateTime());
                    } else {
                        retries--;
                        if (retries >= 0) {
                            String status = "MINIPAS is not ready for import, retrying " + (noOfRetries - retries)
                                    + " of " + noOfRetries + ", waiting " + waitPeriodMinutes + " minutes";
                            currentImportProgress.addStatusLine(status);
                            aLog.warn(status);
                            try {
                                Thread.sleep(waitPeriodMinutes * 60 * 1000);
                            } catch (InterruptedException e) {
                                aLog.error("", e);
                            }
                        } else {
                            String status = "Aborting process, MINIPAS was not ready";
                            currentImportProgress.addStatusLine(status);
                            statusRepo.importEndedWithFailure(new DateTime(), status);
                            aLog.warn(status);
                            doRetry = false;
                        }
                    }
                }
                aLog.info("Importer done");
                currentImportProgress.addStatusLine("done");
            } catch (RuntimeException t) {
                currentImportProgress.addStatusLine("Aborted due to error:");
                String stackTrace = getStackTrace(t);
                currentImportProgress.addStatusLine(stackTrace);
                statusRepo.importEndedWithFailure(new DateTime(), "Aborted due to error:" + stackTrace);
                throw t;
            } finally {
                aLog.info("Stopped processing, manual=" + manual);
                running = false;
            }
        } else {
            aLog.warn("Importer already running");
        }
    }

    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public void doImport() {
        // cleanup - will only cleanup the sync db, not the data already converted and imported
        int currentyear = getYear();
        int firstYear = currentyear - yearstotal + 1;
        currentImportProgress.addStatusLine("cleaning up data older than " + firstYear);
        haibaDao.syncCleanupRowsFromTablesOlderThanYear(firstYear);
        haibaDao.importStarted();
        currentImportProgress.addStatusLine("dots are batches of size " + batchSize);
        // flush previous caches
        minipasDao.reset();
        haibaDao.reset();
        for (int year = firstYear; year <= currentyear; year++) {
            int createdCount = 0;
            int updatedCount = 0;
            int deletedCount = 0;
            int kRecnum = -1;
            String sYear = "" + year;
            currentImportProgress.addStatusLine("processing year " + sYear);
            if (aLog.isDebugEnabled()) {
                aLog.debug("doImport: processing year " + sYear);
            }
            Collection<MinipasTADM> minipasTADM = null;
            while (minipasTADM == null || !minipasTADM.isEmpty()) {
                currentImportProgress.addProgressDot();
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: kRecnum=" + kRecnum + ", batchSize=" + batchSize);
                }
                minipasTADM = minipasDao.getMinipasTADM(sYear, kRecnum, batchSize);
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: minipas returned " + minipasTADM.size());
                }
                haibaDao.setupTransaction();
                MinipasSyncStructure syncStructure = haibaDao.syncTest(year, minipasTADM);
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: tested, got " + syncStructure.getCreated().size() + " created and "
                            + syncStructure.getUpdated().size() + " updated");
                }
                createdCount += syncStructure.getCreated().size();
                updatedCount += syncStructure.getUpdated().size();
                handleCreated(sYear, syncStructure.getCreated());
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: handled created");
                }
                handleUpdated(sYear, syncStructure.getUpdated());
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: handled updated");
                }

                // now remember that we have processed the changes
                haibaDao.syncCommit(year, syncStructure);
                haibaDao.commitTransaction();
                if (aLog.isTraceEnabled()) {
                    aLog.trace("doImport: committed");
                }

                kRecnum = getMaxRecnum(minipasTADM);
            }

            // ask sync dao what we haven't mentioned yet - they are deleted
            Collection<String> deleted = haibaDao.syncGetDeletedIdnummers(year);
            deletedCount += deleted.size();
            // not necessary to handle in transaction - will be deleted on the next run
            handleDeleted(sYear, deleted);
            haibaDao.syncCommitDeleted(year, deleted);
            String status = "year " + year + " done. created:" + createdCount + ", updated:" + updatedCount
                    + ", deleted:" + deletedCount;
            aLog.info(status);
            currentImportProgress.addStatusLine(status);
        }
        haibaDao.importEnded();
    }

    private boolean minipasOk(long lastReturnCodeElseNegativeOne) {
        return lastReturnCodeElseNegativeOne != -1 && lastReturnCodeElseNegativeOne <= 1;
    }

    private boolean minipasError(long lastReturnCodeElseNegativeOne) {
        return lastReturnCodeElseNegativeOne > 1;
    }

    private void handleDeleted(String year, Collection<String> deletedIdnummers) {
        for (String idnummer : deletedIdnummers) {
            haibaDao.clearAdm(idnummer);
            haibaDao.clearKoder(idnummer);
            haibaDao.setDeleted(idnummer);
        }
    }

    private void handleUpdated(String year, Collection<MinipasTADM> updated) {
        Monitor mon = MonitorFactory.start("MinipasPreprocessor.handleUpdated()");
        haibaDao.resetAdmD_IMPORTDTO(updated);
        for (MinipasTADM minipasTADM : updated) {
            haibaDao.clearKoder(minipasTADM.getIdnummer());
            Collection<MinipasTSKSUBE_OPR> ubeoprs = minipasDao.getMinipasSKSUBE_OPR(year, minipasTADM.getK_recnum());
            haibaDao.createKoderFromSksUbeOpr(minipasTADM, ubeoprs);
            Collection<MinipasTDIAG> diags = minipasDao.getMinipasDIAG(year, minipasTADM.getK_recnum());
            haibaDao.createKoderFromDiag(minipasTADM, diags);
        }
        mon.stop();
    }

    private void handleCreated(String year, Collection<MinipasTADM> created) {
        Monitor mon = MonitorFactory.start("MinipasPreprocessor.handleCreated()");
        haibaDao.createAdm(created);
        for (MinipasTADM minipasTADM : created) {
            Collection<MinipasTSKSUBE_OPR> ubeoprs = minipasDao.getMinipasSKSUBE_OPR(year, minipasTADM.getK_recnum());
            haibaDao.createKoderFromSksUbeOpr(minipasTADM, ubeoprs);
            Collection<MinipasTDIAG> diags = minipasDao.getMinipasDIAG(year, minipasTADM.getK_recnum());
            haibaDao.createKoderFromDiag(minipasTADM, diags);
        }
        mon.stop();
    }

    private int getMaxRecnum(Collection<MinipasTADM> minipasTADMs) {
        int returnValue = -1;
        for (MinipasTADM minipasTADM : minipasTADMs) {
            returnValue = Math.max(returnValue, minipasTADM.getK_recnum());
        }
        return returnValue;
    }

    /**
     * Returns year with century, e.g. 2014
     */
    private int getYear() {
        return new DateTime().getYear();
    }

    public boolean isManualOverride() {
        return manualOverride;
    }

    public void setManualOverride(boolean mo) {
        manualOverride = mo;
    }

    public void doManualProcess() {
        doProcess(true);
    }
}
