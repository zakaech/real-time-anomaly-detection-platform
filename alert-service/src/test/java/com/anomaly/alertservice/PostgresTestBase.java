package com.anomaly.alertservice;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A real PostgreSQL for the tests that need one.
 *
 * <p>Testcontainers is used here and nowhere else, and the reason is specific:
 * the guarantee this whole phase rests on is
 * {@code INSERT ... ON CONFLICT ... RETURNING (xmax = 0)}, which is PostgreSQL.
 * Proving idempotence on H2 would prove something about a database we do not
 * ship, and the partial index, the JSONB columns and the CHECK constraints are
 * equally PostgreSQL-specific.
 *
 * <p>The container is static, so one database serves every test class that
 * extends this. Flyway then runs the real migrations against it -- which also
 * means every run of these tests is a test of the migrations themselves.
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public abstract class PostgresTestBase {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16.4-alpine")
                    .withDatabaseName("anomaly")
                    .withUsername("anomaly")
                    .withPassword("test-only-not-a-secret")
                    // Durability is irrelevant in a throwaway test database.
                    .withCommand("postgres", "-c", "fsync=off");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
