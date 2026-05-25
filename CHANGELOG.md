# CHANGELOG

<!-- version list -->

## v2.3.0 (2026-05-25)

### Features

- **CPL-0**: Add extend_path to allow multiple packages with cledar path
  ([`e03ae61`](https://github.com/Cledar/cledar-python-sdk/commit/e03ae6177d4d3d829b07a8c8b5aea2e1441bacbb))


## v2.2.2 (2026-05-07)

### Bug Fixes

- Support Azure default auth for object storage
  ([#37](https://github.com/Cledar/cledar-python-sdk/pull/37),
  [`0000ea6`](https://github.com/Cledar/cledar-python-sdk/commit/0000ea6b6e74197895605f5a775428835c3db092))


## v2.2.1 (2026-03-12)

### Refactoring

- **CPL-559**: Fix project version
  ([`73b358b`](https://github.com/Cledar/cledar-python-sdk/commit/73b358bcdbc5834d1be6b60ae135a90fa10bf940))

- **CPL-559**: Make s3 client optional
  ([`3c65baf`](https://github.com/Cledar/cledar-python-sdk/commit/3c65bafbd562a24dab67abf7ecc4c15968c90f20))

- **CPL-559**: Test release tag update
  ([`ab375ea`](https://github.com/Cledar/cledar-python-sdk/commit/ab375ea539e4f40bb2f7f2a0dcc18cfc7f247f88))


## v2.2.0 (2026-02-19)

### Build System

- **deps**: Bump aiohttp from 3.13.0 to 3.13.3
  ([#18](https://github.com/Cledar/cledar-python-sdk/pull/18),
  [`71c873d`](https://github.com/Cledar/cledar-python-sdk/commit/71c873d425a9a4e814f824b9158be2c30775acd3))

- **deps**: Bump cryptography from 46.0.3 to 46.0.5
  ([#24](https://github.com/Cledar/cledar-python-sdk/pull/24),
  [`d687b31`](https://github.com/Cledar/cledar-python-sdk/commit/d687b3106152663e4be6aa2f27f34b5a155cdbaa))

### Features

- **CPL-268**: Add asynchronous object storage functionality and corresponding tests
  ([`2ecbc02`](https://github.com/Cledar/cledar-python-sdk/commit/2ecbc02346688dc90d0419a69c8db5e9fa848e15))


## v2.1.4 (2026-02-17)

### Bug Fixes

- **CPL-240**: Allow awaitable health checks in monitoring context
  ([#23](https://github.com/Cledar/cledar-python-sdk/pull/23),
  [`32f2373`](https://github.com/Cledar/cledar-python-sdk/commit/32f23739ac0979fffcf57e4592053f58acbce2d4))


## v2.1.3 (2026-02-17)

### Bug Fixes

- **CPL-256**: Introduce type aliases for monitoring checks
  ([#22](https://github.com/Cledar/cledar-python-sdk/pull/22),
  [`4d33fd4`](https://github.com/Cledar/cledar-python-sdk/commit/4d33fd43a6d6480c951edee90da2e2d47332bd37))


## v2.1.2 (2026-02-06)

### Bug Fixes

- **CPL-320**: Set default value for `s3_max_concurrency` in object storage models
  ([#21](https://github.com/Cledar/cledar-python-sdk/pull/21),
  [`d88e44f`](https://github.com/Cledar/cledar-python-sdk/commit/d88e44f5f56208317d33ebf127fddc6d0e81e10a))


## v2.1.1 (2026-02-06)

### Bug Fixes

- **CPL-314**: Add support for redis_url parsing in RedisServiceConfig
  ([`e1cd27e`](https://github.com/Cledar/cledar-python-sdk/commit/e1cd27e1c6a1b370c6120dbc49095693a9220491))

- **CPL-314**: Move Redis URL parser into SDK
  ([`e1cd27e`](https://github.com/Cledar/cledar-python-sdk/commit/e1cd27e1c6a1b370c6120dbc49095693a9220491))

### Refactoring

- **CPL-314**: Enable arbitrary types in FailedValue dataclass
  ([`e1cd27e`](https://github.com/Cledar/cledar-python-sdk/commit/e1cd27e1c6a1b370c6120dbc49095693a9220491))

- **CPL-314**: Replace `dataclasses` with `pydantic.dataclasses`
  ([`e1cd27e`](https://github.com/Cledar/cledar-python-sdk/commit/e1cd27e1c6a1b370c6120dbc49095693a9220491))


## v2.1.0 (2026-02-04)

### Features

- **CPL-178**: Add detailed docstrings across the codebase and update configurations
  ([`e2a01ea`](https://github.com/Cledar/cledar-python-sdk/commit/e2a01ea44c68c41c1764992ac7d532d263dc56e5))


## v2.0.3 (2025-12-09)

### Bug Fixes

- **CPL-302**: Include cledar
  ([`3abd44e`](https://github.com/Cledar/cledar-python-sdk/commit/3abd44e0ed5b1e2e09c5ed012599a94535bf1748))


## v2.0.2 (2025-12-09)

### Bug Fixes

- **CPL-302**: Include init
  ([`6eb529d`](https://github.com/Cledar/cledar-python-sdk/commit/6eb529d844701fa2951ec0f382c9e1d7a99ee161))


## v2.0.1 (2025-12-09)

### Bug Fixes

- **CPL-302**: Update build
  ([`a17cd0f`](https://github.com/Cledar/cledar-python-sdk/commit/a17cd0fba354b7e5488782e60fcb6cd6e888c805))


## v2.0.0 (2025-12-09)

### Continuous Integration

- **CPL-0**: Add sem ver dry run
  ([`3beb1f4`](https://github.com/Cledar/cledar-python-sdk/commit/3beb1f46778643ae2536e08fa11d38dba47606c1))

- **CPL-0**: Remove unused config
  ([`9619559`](https://github.com/Cledar/cledar-python-sdk/commit/9619559b39587d4ef885a1dc8822f8f428cb9f4d))

### Features

- **CPL-302**: Add cledar prefix to all packages\
  ([`d632996`](https://github.com/Cledar/cledar-python-sdk/commit/d6329960964dcec28d5c7cdcf4f9abc970331c34))

- **CPL-302**: Update paths
  ([`17bd1f4`](https://github.com/Cledar/cledar-python-sdk/commit/17bd1f41629deb53f3d0920f165ca09d2ae6e46d))


## v1.4.0 (2025-12-02)

### Features

- **CPL-242**: Add Kafka SASL support ([#11](https://github.com/Cledar/cledar-python-sdk/pull/11),
  [`1db2b5b`](https://github.com/Cledar/cledar-python-sdk/commit/1db2b5b0fe8b55e5698134a1f9dfbd833cc705ea))


## v1.3.0 (2025-11-20)

### Features

- **CPL-184**: Add async support to Redis service module
  ([#10](https://github.com/Cledar/cledar-python-sdk/pull/10),
  [`97ee651`](https://github.com/Cledar/cledar-python-sdk/commit/97ee651ca0b4e4db23e899de785361c441a29d9a))

- **CPL-184**: Update test fixture to use pytest-asyncio
  ([#10](https://github.com/Cledar/cledar-python-sdk/pull/10),
  [`97ee651`](https://github.com/Cledar/cledar-python-sdk/commit/97ee651ca0b4e4db23e899de785361c441a29d9a))


## v1.2.1 (2025-11-06)

### Bug Fixes

- **CPL-183**: Update pyproject ([#9](https://github.com/Cledar/cledar-python-sdk/pull/9),
  [`5c231b9`](https://github.com/Cledar/cledar-python-sdk/commit/5c231b94aae67a6843737e8971738754708f46fb))


## v1.2.0 (2025-11-05)

### Bug Fixes

- **wait-for-test-finish**: Streamline release with CI workflow updates
  ([#6](https://github.com/Cledar/cledar-python-sdk/pull/6),
  [`d51a94a`](https://github.com/Cledar/cledar-python-sdk/commit/d51a94af1b31966e0f51736d8e16ad0f6e567809))

### Continuous Integration

- Add deploy keys and enforce successful checks before release
  ([#6](https://github.com/Cledar/cledar-python-sdk/pull/6),
  [`d51a94a`](https://github.com/Cledar/cledar-python-sdk/commit/d51a94af1b31966e0f51736d8e16ad0f6e567809))

- Configure Git identity and remote for GitHub Actions
  ([`d2c8044`](https://github.com/Cledar/cledar-python-sdk/commit/d2c8044a627606f919b81a69b94c98de06248229))

### Features

- **CPL-0**: Remove upper version limit in dependencies
  ([#7](https://github.com/Cledar/cledar-python-sdk/pull/7),
  [`3cb1f7c`](https://github.com/Cledar/cledar-python-sdk/commit/3cb1f7c745a51aa6458efeaa7bde991178709833))

- **CPL-0**: Update dependencies ([#7](https://github.com/Cledar/cledar-python-sdk/pull/7),
  [`3cb1f7c`](https://github.com/Cledar/cledar-python-sdk/commit/3cb1f7c745a51aa6458efeaa7bde991178709833))

- **wait-for-test-finish**: Add ssh-key for secure repo access in release
  ([#6](https://github.com/Cledar/cledar-python-sdk/pull/6),
  [`d51a94a`](https://github.com/Cledar/cledar-python-sdk/commit/d51a94af1b31966e0f51736d8e16ad0f6e567809))


## v1.1.0 (2025-11-04)

### Features

- **CPL-180**: Update readme and python version
  ([#5](https://github.com/Cledar/cledar-python-sdk/pull/5),
  [`deb1668`](https://github.com/Cledar/cledar-python-sdk/commit/deb1668b715e3d129b7c8ad59c560da4b71bfb04))

### Refactoring

- **CPL-180**: Update readme ([#5](https://github.com/Cledar/cledar-python-sdk/pull/5),
  [`deb1668`](https://github.com/Cledar/cledar-python-sdk/commit/deb1668b715e3d129b7c8ad59c560da4b71bfb04))


## v1.0.1 (2025-11-04)

### Bug Fixes

- **update-uvlock**: Bump `cledar-sdk` version to 1.0.0
  ([#4](https://github.com/Cledar/cledar-python-sdk/pull/4),
  [`3b8633e`](https://github.com/Cledar/cledar-python-sdk/commit/3b8633ed02107f27f5fbc9188464cb944d523411))


## v1.0.0 (2025-11-04)

- Initial Release
