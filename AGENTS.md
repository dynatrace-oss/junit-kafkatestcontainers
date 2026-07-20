# AGENTS.md

## Purpose

This repository contains **junit-kafkatestcontainers**, a Java library by Dynatrace that simplifies Kafka integration testing through a single configurable JUnit 5 annotation.

Agents working in this repository should optimize for:

- correctness and reliability of the Kafka test infrastructure
- clear, minimal public API — the annotation and its parameters are user-facing
- full test coverage (JaCoCo enforces 100% line and branch coverage)
- keeping the library usable in both Spring and non-Spring JUnit 5 projects

## Project Overview

`junit-kafkatestcontainers` provides the `@KafkaTestcontainers` annotation and a JUnit 5 extension that manages a real Kafka broker in a Docker container via [Testcontainers](https://testcontainers.com/). It is an alternative to Spring's `@EmbeddedKafka` that lets you choose the Kafka broker version and runs a full broker rather than a minimal in-process one.

Published to Maven Central as `com.dynatrace.junit.kafkatestcontainers:junit-kafkatestcontainers`.

Both `README.md` (user-facing usage guide and contribution instructions) and `CLAUDE.md` (agent instructions) are important reference documents. Read them before making changes.

## Tech Stack

- **Language:** Java 17+ (library baseline; also tested on Java 21 and 25)
- **Build system:** Gradle — use `./gradlew` (Linux/Mac) or `gradlew.bat` (Windows)
- **Testing:** JUnit 5, AssertJ, Awaitility, Testcontainers
- **Static analysis:** Checkstyle (Google Java Format), ErrorProne, NullAway, ForbiddenApis
- **Code coverage:** JaCoCo (100% line and branch coverage required)
- **Benchmarking:** JMH (comparing `@KafkaTestcontainers` vs Spring's `@EmbeddedKafka`)
- **Documentation linting:** markdownlint via Docker (`make markdownlint`)
- **Spring integration:** Spring Boot, Spring Kafka (optional — the annotation works without Spring)

## Project Structure

```shell
src/
  testFixtures/java/    # Library source — annotation, JUnit 5 extension, Spring integration
  test/java/
    junit/              # Integration tests for plain JUnit 5 usage
    spring/             # Integration tests for Spring usage
  jmh/java/             # JMH benchmarks (EmbeddedKafka vs Testcontainers comparisons)
benchmark_result/       # Stored JMH benchmark result JSONs
config/
  checkstyle/           # Checkstyle configuration
  forbiddenapis/        # ForbiddenApis signature files
.github/workflows/      # CI: build, release, Gradle wrapper validation, markdownlint
scripts/                # Utility scripts (e.g. version check)
```

Key classes in `src/testFixtures/java/com/dynatrace/junit/kafkatestcontainers/`:

- `KafkaTestcontainers.java` — the annotation (public API)
- `KafkaTestcontainersExtension.java` — JUnit 5 extension managing container lifecycle
- `KafkaTestcontainersContextCustomizer.java` / `KafkaTestcontainersContextCustomizerFactory.java` — Spring context integration
- `KafkaTestcontainersUtils.java` — internal utilities

> **Why `src/testFixtures/`?** The library is published as a [Gradle test fixture](https://docs.gradle.org/current/userguide/java_test_fixtures.html) because the annotation is inherently test-scoped — consumers add it via `testImplementation(testFixtures(...))`. There is no `src/main/java`. Do not move production code there.
<!-- -->
> **Container lifecycle:** `KafkaTestcontainersExtension` starts one container per test class (before all test methods) and tears it down after all methods complete. This one-container-per-class guarantee is the core invariant of the extension. Preserve it when modifying the extension.

## Common Commands

| Command | Description |
| --- | --- |
| `./gradlew build` | Compile, run all checks, and run tests on Java 17, 21, and 25 |
| `./gradlew test` | Run integration tests with Java 17 only |
| `./gradlew testJava21` | Run integration tests with Java 21 only |
| `./gradlew testJava25` | Run integration tests with Java 25 only |
| `./gradlew build buildHealth` | Full check suite including dependency analysis |
| `./gradlew checkstyleAll` | Verify Google Java Style across all source sets |
| `./gradlew jacocoTestReport` | Generate JaCoCo coverage report |
| `./gradlew jmh` | Run JMH benchmarks (results written to `benchmark_result/result.json`) |
| `make markdownlint` | Lint all Markdown files (requires Docker) |
| `make markdownlint-fix` | Auto-fix Markdown lint issues (requires Docker) |

A running Docker daemon is required for tests and `markdownlint`. Set `DOCKER_HOST` if your daemon uses a non-default socket.

## Repository expectations

- Keep changes simple, explicit, and easy to review.
- Prefer small, focused pull requests — one concern per PR.
- Preserve the public API contract; the annotation parameters are user-facing.
- All new code must be fully covered — JaCoCo enforces 100% line and branch coverage.
- Run `./gradlew checkstyleAll` before committing; formatting issues will fail CI.
- Make ownership and support expectations explicit.

## Required baseline files

Unless the task explicitly says otherwise, preserve or improve these files:

- `README.md`
- `LICENSE`
- `NOTICE`
- `CODEOWNERS`
- `SUPPORT.md`
- `.github/PULL_REQUEST_TEMPLATE.md`
- `.github/dependabot.yml`
- `.github/workflows/`
- `.github/ISSUE_TEMPLATE/`
- `AGENTS.md`
- `CLAUDE.md`
- `.github/copilot-instructions.md`

## Documentation guidance

- Treat the root `README.md` as the user-facing guide: usage, configuration, and migration instructions.
- Prefer concrete, action-oriented instructions.
- Use policy-style wording where expectations are mandatory.
- Keep support and ownership language explicit.
- Keep code examples short and copy-paste ready.
- Markdown files must pass `make markdownlint` before merging.

## Workflow guidance

Before proposing changes:

- check whether the public annotation API is affected — any parameter addition, removal, or rename is a breaking change
- verify Docker is available if the change touches test infrastructure or documentation linting
- preserve review-friendly, focused workflows
- avoid unnecessary complexity in the extension lifecycle or Spring wiring

## Pull request guidance

When preparing a pull request:

- summarize what changed and why
- call out any changes to the public annotation API or Spring property keys
- confirm `./gradlew build buildHealth` passes and coverage remains at 100%
- confirm `make markdownlint` passes if any Markdown was edited
- keep the scope focused and easy to review

## Review checklist

When reviewing changes to this repository, verify that:

- `CODEOWNERS` is present
- support expectations are documented
- no secrets or environment-specific values are included
- all new code has tests and JaCoCo coverage remains complete
- Checkstyle, ErrorProne, NullAway, and ForbiddenApis checks pass
- the annotation API and Spring property key defaults are not changed without clear intent
- Markdown files pass markdownlint

## What to avoid

- Do not break the plain JUnit 5 usage path when changing Spring integration, or vice versa.
- Do not add `System.out` calls — use a logger (enforced by ForbiddenApis).
- Do not add nullable fields or parameters in production code without `@org.jspecify.annotations.*` annotations — NullAway is enforced.
- Do not assume this library is commercially supported; it is an experimental OSS project.
- Do not add heavy automation or dependencies unless they are clearly justified.
