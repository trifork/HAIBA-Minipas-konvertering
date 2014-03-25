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
package dk.nsi.haiba.minipasconverter;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.executor.MinipasPreprocessor;
import dk.nsi.haiba.minipasconverter.status.CurrentImportProgress;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
@Ignore // not able to run from maven without db2 driver
public class PreprocessorTest {
    @Configuration
    @Import({ TestConfiguration.class })
    static class MyConfiguration {
        @Bean
        public CurrentImportProgress currentImportProgress() {
            return Mockito.mock(CurrentImportProgress.class);
        }
    }

    @Autowired
    MinipasDAO minipasDAO;

    @Autowired
    MinipasPreprocessor minipasPreprocessor;

    @Autowired
    CurrentImportProgress currentImportProgress;

    @Autowired
    @Qualifier("minipasJdbcTemplate")
    JdbcTemplate minipasJdbc;

    @Test
    public void testEmptyImport() {
        minipasJdbc.update("delete from T_MINIPAS_UGL_STATUS");
        // insert ok status
        minipasJdbc.update("insert into T_MINIPAS_UGL_STATUS (K_ID, D_STARTDATETIME, D_ENDDATETIME, V_RETURNCODE) VALUES (1, '2014-03-10', '2014-03-11', 1)");
        minipasPreprocessor.doManualProcess();
        assertTrue(true);
    }
}
