# Changelog

## [v0.3.0] - 2026-11-02

### 🌟 Features

- Support reading offsets from a restored __consumer_offsets topic ([#45](https://github.com/kannika-io/kbridge/pull/45))
- Show progress while reading the offsets topic ([#53](https://github.com/kannika-io/kbridge/pull/53))
- Show progress while mapping partitions ([#63](https://github.com/kannika-io/kbridge/pull/63))

### 🐛 Bug Fixes

- Summarize apply confirmation/dry-run output and bound CSV memory ([#48](https://github.com/kannika-io/kbridge/pull/48))
- Do not publish prerelease images as stable tags ([#51](https://github.com/kannika-io/kbridge/pull/51))
- Handle commands while a partition channel is full ([#59](https://github.com/kannika-io/kbridge/pull/59))
- Read apply-offsets input from stdin with -i - ([#62](https://github.com/kannika-io/kbridge/pull/62))
- Map consumer groups on empty target partitions to the high watermark ([#57](https://github.com/kannika-io/kbridge/pull/57))
- Run blocking librdkafka calls on the blocking pool ([#65](https://github.com/kannika-io/kbridge/pull/65))
- Fail on non-increasing legacy offsets instead of panicking ([#68](https://github.com/kannika-io/kbridge/pull/68))
- Report failed seeks as errors and skip the Seeked event ([#64](https://github.com/kannika-io/kbridge/pull/64))

### 📚 Documentation

- Document required Kafka permissions ([#42](https://github.com/kannika-io/kbridge/pull/42))

### 🧹 Miscellaneous Tasks

- Time out the check job after 20 minutes ([#58](https://github.com/kannika-io/kbridge/pull/58))

**Full Changelog**: https://github.com/kannika-io/kbridge/compare/v0.2.0...v0.3.0
