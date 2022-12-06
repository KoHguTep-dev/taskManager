package loadmanager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(
        scanBasePackages = {"", ""},
        exclude = {DataSourceAutoConfiguration.class, LiquibaseAutoConfiguration.class}
)
@EnableScheduling
public class LoadManagerMsApplication {
    public static void main(String[] args) {
        SpringApplication.run(LoadManagerMsApplication.class, args);
    }
}
