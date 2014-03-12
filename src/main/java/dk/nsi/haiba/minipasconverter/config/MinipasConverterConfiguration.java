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
package dk.nsi.haiba.minipasconverter.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jndi.JndiObjectFactoryBean;

import dk.nsi.haiba.minipasconverter.dao.MinipasDAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasHAIBADAO;
import dk.nsi.haiba.minipasconverter.dao.MinipasSyncDAO;
import dk.nsi.haiba.minipasconverter.dao.impl.MinipasDAOImpl;
import dk.nsi.haiba.minipasconverter.dao.impl.MinipasHAIBADAOImpl;
import dk.nsi.haiba.minipasconverter.dao.impl.MinipasSyncDAOImpl;
import dk.nsi.haiba.minipasconverter.executor.MinipasPreprocessor;
import dk.nsi.haiba.minipasconverter.status.CurrentImportProgress;
import dk.nsi.haiba.minipasconverter.status.ImportStatusRepository;
import dk.nsi.haiba.minipasconverter.status.ImportStatusRepositoryJdbcImpl;
import dk.nsi.haiba.minipasconverter.status.TimeSource;
import dk.nsi.haiba.minipasconverter.status.TimeSourceRealTimeImpl;

@Configuration
public class MinipasConverterConfiguration {
    @Value("${jdbc.haibaJNDIName}")
    private String haibaJNDIName;

    @Value("${jdbc.haibaSyncJNDIName}")
    private String haibaSyncJNDIName;

    @Value("${jdbc.minipasJNDIName}")
    private String minipasJNDIName;

    @Value("${jdbc.haiba.dialect:MSSQL}")
    private String haibadialect;

    @Value("${jdbc.haibasync.dialect:MSSQL}")
    private String haibasyncdialect;

    // this is not automatically registered, see https://jira.springsource.org/browse/SPR-8539
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        propertySourcesPlaceholderConfigurer.setIgnoreResourceNotFound(true);
        propertySourcesPlaceholderConfigurer.setIgnoreUnresolvablePlaceholders(false);

        propertySourcesPlaceholderConfigurer
                .setLocations(new Resource[] { new ClassPathResource("default-config.properties"),
                        new ClassPathResource("minipasconfig.properties") });

        return propertySourcesPlaceholderConfigurer;
    }

    @Bean
    public DataSource haibaDataSource() throws Exception {
        JndiObjectFactoryBean factory = new JndiObjectFactoryBean();
        factory.setJndiName(haibaJNDIName);
        factory.setExpectedType(DataSource.class);
        factory.afterPropertiesSet();
        return (DataSource) factory.getObject();
    }

    @Bean
    public DataSource haibaSyncDataSource() throws Exception {
        JndiObjectFactoryBean factory = new JndiObjectFactoryBean();
        factory.setJndiName(haibaSyncJNDIName);
        factory.setExpectedType(DataSource.class);
        factory.afterPropertiesSet();
        return (DataSource) factory.getObject();
    }

    @Bean
    public DataSource minipasDataSource() throws Exception {
        JndiObjectFactoryBean factory = new JndiObjectFactoryBean();
        factory.setJndiName(minipasJNDIName);
        factory.setExpectedType(DataSource.class);
        factory.afterPropertiesSet();
        return (DataSource) factory.getObject();
    }

    @Bean
    public JdbcTemplate haibaJdbcTemplate(@Qualifier("haibaDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public JdbcTemplate haibaSyncJdbcTemplate(@Qualifier("haibaSyncDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public JdbcTemplate minipasJdbcTemplate(@Qualifier("minipasDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public MinipasPreprocessor minipasPreprocessor() {
        return new MinipasPreprocessor();
    }

    @Bean
    public ImportStatusRepository importStatusRepository() {
        return new ImportStatusRepositoryJdbcImpl(haibadialect);
    }

    @Bean
    public TimeSource timeSource() {
        return new TimeSourceRealTimeImpl();
    }

    @Bean
    public MinipasDAO minipasDAO() {
        return new MinipasDAOImpl();
    }

    @Bean
    public MinipasHAIBADAO minipasHAIBADAO() {
        return new MinipasHAIBADAOImpl(haibadialect);
    }

    @Bean
    public MinipasSyncDAO minipasSyncDAO() {
        return new MinipasSyncDAOImpl(haibasyncdialect);
    }

    @Bean
    public CurrentImportProgress currentImportProgress() {
        return new CurrentImportProgress();
    }
}
