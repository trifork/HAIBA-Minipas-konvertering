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

import java.sql.Driver;

import javax.sql.DataSource;

import org.junit.Ignore;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import dk.nsi.haiba.minipasconverter.config.MinipasConverterConfiguration;

@Configuration
@EnableTransactionManagement
@PropertySource("test.properties")
public class TestConfiguration extends MinipasConverterConfiguration {
    @Bean
    public DataSource minipasDataSource() throws Exception {
        String jdbcUrlPrefix = "jdbc:db2://127.0.0.1:50000/HAIBA";

        // not in maven, only works in eclipse
        Driver d = (Driver) Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
        return new SimpleDriverDataSource(d, jdbcUrlPrefix, "db2inst1", "db2inst1");
    }

    @Bean
    public DataSource haibaDataSource() throws Exception {
        return Mockito.mock(DataSource.class);
    }

    @Bean
    public DataSource haibaSyncDataSource() throws Exception {
        return Mockito.mock(DataSource.class);
    }

    @Bean
    public JdbcTemplate minipasJdbcTemplate(@Qualifier("minipasDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public JdbcTemplate minipasSyncJdbcTemplate() {
        return Mockito.mock(JdbcTemplate.class);
    }

    @Bean
    public JdbcTemplate minipasHaibaJdbcTemplate() {
        return Mockito.mock(JdbcTemplate.class);
    }
}




