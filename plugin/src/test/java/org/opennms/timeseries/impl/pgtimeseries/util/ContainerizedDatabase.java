package org.opennms.timeseries.impl.pgtimeseries.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A minimal, container-runtime-agnostic PostgreSQL launcher for integration tests.
 *
 * <p>Testcontainers requires the Docker HTTP API, which containerd does not expose. This helper instead drives
 * whatever container CLI is present so the integration tests run regardless of runtime:</p>
 * <ul>
 *   <li>{@code docker} / {@code nerdctl} / {@code podman}: started with published port mapping;</li>
 *   <li>{@code ctr} (containerd's native CLI): started with {@code --net-host} (it has no built-in port
 *       publishing), so the database is reached on the host port directly.</li>
 * </ul>
 *
 * <p>The runtime is auto-detected (override with {@code -Dpgtimeseries.test.runtime=docker|nerdctl|podman|ctr}).
 * Readiness is detected by polling a JDBC connection rather than parsing logs, which works for every runtime.</p>
 */
public final class ContainerizedDatabase implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerizedDatabase.class);

    private static final String USER = "postgres";
    private static final String PASSWORD = "password";
    private static final String DB = "postgres";
    private static final String[] CANDIDATES = {"docker", "nerdctl", "podman", "ctr"};

    /**
     * Single source of truth for the pg_timeseries test image. Override per-run with
     * {@code -Dpgtimeseries.test.image=...}. Both integration tests use this so the tag is bumped in one place.
     */
    public static final String DEFAULT_IMAGE =
            System.getProperty("pgtimeseries.test.image", "ghcr.io/chuckhend/pg18-timeseries:latest");

    private final String cli;
    private final String containerRef; // container id (docker-like) or name (ctr)
    private final String jdbcUrl;

    private ContainerizedDatabase(String cli, String containerRef, String jdbcUrl) {
        this.cli = cli;
        this.containerRef = containerRef;
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Detects a usable container runtime. Each candidate is probed with {@code <cli> version}, which contacts
     * the daemon/socket, so a CLI that is installed but whose daemon is not running (e.g. {@code ctr} without a
     * containerd socket) is correctly treated as unavailable. An explicit
     * {@code -Dpgtimeseries.test.runtime=...} is trusted without probing.
     *
     * @return the detected runtime CLI, or {@code null} if none is usable.
     */
    public static String detectRuntime() {
        String override = System.getProperty("pgtimeseries.test.runtime");
        if (override != null && !override.trim().isEmpty()) {
            return override.trim();
        }
        for (String candidate : CANDIDATES) {
            // a missing binary makes ProcessBuilder throw -> run() returns exitCode -1, so it is skipped
            if (run(10_000, candidate, "version").exitCode == 0) {
                return candidate;
            }
        }
        return null;
    }

    public static boolean isRuntimeAvailable() {
        return detectRuntime() != null;
    }

    /** Starts a container using {@link #DEFAULT_IMAGE}. */
    public static ContainerizedDatabase start() {
        return start(DEFAULT_IMAGE);
    }

    /** Pulls (if needed), starts, and waits for a PostgreSQL container, returning a handle to it. */
    public static ContainerizedDatabase start(String image) {
        String cli = detectRuntime();
        if (cli == null) {
            throw new IllegalStateException("No supported container runtime found");
        }
        ContainerizedDatabase db = "ctr".equals(cli) ? startCtr(image) : startDockerLike(cli, image);
        db.waitUntilReady(120_000);
        return db;
    }

    private static ContainerizedDatabase startDockerLike(String cli, String image) {
        // Note: no --rm, so that if the container exits during startup its logs survive for diagnostics;
        // close() removes it explicitly.
        ExecResult run = run(300_000, cli, "run", "-d",
                "-e", "POSTGRES_PASSWORD=" + PASSWORD,
                "-p", "127.0.0.1::5432", image);
        if (run.exitCode != 0) {
            throw new IllegalStateException("Failed to start container via " + cli + ": " + run.stderr);
        }
        String id = run.stdout.trim();
        ExecResult portResult = run(30_000, cli, "port", id, "5432");
        int port = parseHostPort(portResult.stdout);
        String url = String.format("jdbc:postgresql://127.0.0.1:%d/%s", port, DB);
        LOG.info("Started PostgreSQL via {} (container {}) at {}", cli, id, url);
        return new ContainerizedDatabase(cli, id, url);
    }

    private static ContainerizedDatabase startCtr(String image) {
        ExecResult pull = run(300_000, "ctr", "image", "pull", image);
        if (pull.exitCode != 0) {
            throw new IllegalStateException("Failed to pull image via ctr: " + pull.stderr);
        }
        String name = "pgts-test-" + UUID.randomUUID().toString().substring(0, 8);
        // ctr has no port publishing, so share the host network and reach postgres on the host port directly.
        ExecResult run = run(60_000, "ctr", "run", "-d", "--net-host",
                "--env", "POSTGRES_PASSWORD=" + PASSWORD, image, name);
        if (run.exitCode != 0) {
            throw new IllegalStateException("Failed to start container via ctr: " + run.stderr);
        }
        int port = Integer.getInteger("pgtimeseries.test.port", 5432);
        String url = String.format("jdbc:postgresql://127.0.0.1:%d/%s", port, DB);
        LOG.info("Started PostgreSQL via ctr (container {}, host network) at {}", name, url);
        return new ContainerizedDatabase("ctr", name, url);
    }

    /** Parses a host port out of "docker/nerdctl port" output such as "127.0.0.1:49154" or "5432/tcp -> 0.0.0.0:49154". */
    static int parseHostPort(String portOutput) {
        for (String line : portOutput.split("\\R")) {
            int colon = line.lastIndexOf(':');
            if (colon >= 0 && colon < line.length() - 1) {
                String tail = line.substring(colon + 1).trim();
                try {
                    return Integer.parseInt(tail);
                } catch (NumberFormatException ignore) {
                    // try next line
                }
            }
        }
        throw new IllegalStateException("Could not parse published port from: " + portOutput);
    }

    private void waitUntilReady(long timeoutMs) {
        // The driver logs a noisy WARNING (e.g. "Broken pipe") on every failed connect attempt while the
        // database is still starting; quiet it so the polling does not flood the test output.
        java.util.logging.Logger.getLogger("org.postgresql").setLevel(Level.SEVERE);

        long deadline = System.currentTimeMillis() + timeoutMs;
        SQLException last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection c = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
                if (c.isValid(5)) {
                    return;
                }
            } catch (SQLException e) {
                last = e;
            }
            // Fail fast (rather than polling until timeout) if the container died during startup, and include
            // its logs so the real cause is visible instead of a generic timeout.
            if (hasExited()) {
                String logs = diagnostics();
                close();
                throw new IllegalStateException("Database container exited during startup:\n" + logs, last);
            }
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        String logs = diagnostics();
        close();
        throw new IllegalStateException(
                "Database did not become ready within " + timeoutMs + "ms. Container logs:\n" + logs, last);
    }

    private boolean dockerLike() {
        return !"ctr".equals(cli);
    }

    /** @return true if the container is no longer running (exited or already removed). */
    private boolean hasExited() {
        if (dockerLike()) {
            ExecResult r = run(10_000, cli, "inspect", "--format", "{{.State.Status}}", containerRef);
            if (r.exitCode != 0) {
                return true; // not found -> gone/removed -> treat as exited
            }
            return !r.stdout.trim().equalsIgnoreCase("running");
        }
        // ctr: our task should be present and RUNNING
        ExecResult r = run(10_000, "ctr", "task", "ls");
        if (r.exitCode != 0) {
            return false; // can't tell; let the timeout handle it
        }
        for (String line : r.stdout.split("\\R")) {
            if (line.contains(containerRef)) {
                return !line.contains("RUNNING");
            }
        }
        return true; // task not listed -> not running
    }

    private String diagnostics() {
        if (dockerLike()) {
            ExecResult r = run(15_000, cli, "logs", containerRef);
            String out = (r.stdout + "\n" + r.stderr).trim();
            return out.isEmpty() ? "(no container logs available)" : out;
        }
        return "(container logs are not readily available via ctr)";
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String username() {
        return USER;
    }

    public String password() {
        return PASSWORD;
    }

    @Override
    public void close() {
        try {
            if ("ctr".equals(cli)) {
                run(15_000, "ctr", "task", "kill", "-s", "SIGKILL", containerRef);
                run(15_000, "ctr", "task", "delete", containerRef);
                run(15_000, "ctr", "container", "delete", containerRef);
            } else {
                run(30_000, cli, "rm", "-f", containerRef);
            }
        } catch (RuntimeException e) {
            LOG.warn("Failed to clean up container {} via {}", containerRef, cli, e);
        }
    }

    // ---- process execution -------------------------------------------------

    private static ExecResult run(long timeoutMs, String... cmd) {
        Process p = null;
        try {
            p = new ProcessBuilder(cmd).start();
            StreamGobbler out = new StreamGobbler(p.getInputStream());
            StreamGobbler err = new StreamGobbler(p.getErrorStream());
            Thread to = new Thread(out);
            Thread te = new Thread(err);
            to.start();
            te.start();
            boolean finished = p.waitFor(timeoutMs, TimeUnit.MILLISECONDS);
            if (!finished) {
                p.destroyForcibly();
            }
            to.join(2_000);
            te.join(2_000);
            return new ExecResult(finished ? p.exitValue() : -1, out.text(), err.text());
        } catch (IOException | InterruptedException e) {
            if (p != null) {
                p.destroyForcibly();
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new ExecResult(-1, "", e.toString());
        }
    }

    private static final class ExecResult {
        final int exitCode;
        final String stdout;
        final String stderr;

        ExecResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }

    private static final class StreamGobbler implements Runnable {
        private final InputStream in;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        StreamGobbler(InputStream in) {
            this.in = in;
        }

        @Override
        public void run() {
            byte[] chunk = new byte[4096];
            int read;
            try {
                while ((read = in.read(chunk)) != -1) {
                    buffer.write(chunk, 0, read);
                }
            } catch (IOException ignore) {
                // process ended / stream closed
            }
        }

        String text() {
            return buffer.toString();
        }
    }
}
