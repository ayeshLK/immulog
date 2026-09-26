# Changelog

## [0.3.0](https://github.com/ayeshLK/immulog/compare/v0.2.0...v0.3.0) (2026-09-26)


### Features

* support native macOS and Windows runtimes ([6b98919](https://github.com/ayeshLK/immulog/commit/6b98919d5629e01414bfb17a54e7f9ef4305c5b1))
* support native macOS and Windows runtimes ([fe195b0](https://github.com/ayeshLK/immulog/commit/fe195b0578aadd261f72e50c2963ae101901e429))


### Bug Fixes

* close temporary snapshots on Windows ([ce6b611](https://github.com/ayeshLK/immulog/commit/ce6b611b4592fce8e297ab8d45d61a833c872004))
* support directory sync on macOS and Windows ([acbd12b](https://github.com/ayeshLK/immulog/commit/acbd12bc6fdabdbcce37777fc01954e47d86100b))

## [0.2.0](https://github.com/ayeshLK/immulog/compare/v0.1.0...v0.2.0) (2026-09-26)


### Features

* add durable append-only event log ([4b03bfa](https://github.com/ayeshLK/immulog/commit/4b03bfa61e290e13ce9206e83555196cfaf32ac3))


### Bug Fixes

* add headers to benchmark tests ([5ed8eaa](https://github.com/ayeshLK/immulog/commit/5ed8eaae5776c6f5a9fb51d8bbc70e90a8835d5f))
* address review feedback on descriptor cap and cache sizing ([cba7110](https://github.com/ayeshLK/immulog/commit/cba71105a3509e8ffa70feec8739e07376d0ac56))
* bound soak harness oracle memory and clarify retention accounting ([98d87b6](https://github.com/ayeshLK/immulog/commit/98d87b6edaf0b28a77a6dd9b9e25f9f7fd3e9eb5))
* bound soak oracle memory ([e3e2796](https://github.com/ayeshLK/immulog/commit/e3e2796eff89f95972a04bf8f42ed3027b2eb992))
* bound soak oracle memory ([3d1bfda](https://github.com/ayeshLK/immulog/commit/3d1bfdaaaf0a5b693c89e4cf471bc1504854e28b))
* keep active consumer leases alive ([5a1708c](https://github.com/ayeshLK/immulog/commit/5a1708cd5c7f94e83c95b49aa597b6d556f4d897))
* keep active consumer leases alive ([9159b00](https://github.com/ayeshLK/immulog/commit/9159b001f538c9613c3dadb3eceb27e39d50ac0c))
* keep soak oracle checks off consumer path ([d9bb868](https://github.com/ayeshLK/immulog/commit/d9bb8689ed9faeb8a2ad67fd8d9e5504d9ab4b41))
* keep soak oracle checks off consumer path ([8b9c985](https://github.com/ayeshLK/immulog/commit/8b9c985e3d55418ed8f7514621c9a85f1c2e839d))
* make consumer stats fully non-blocking ([9a232db](https://github.com/ayeshLK/immulog/commit/9a232db1f84c3c0850a1399a337d85476ec55339))
* make consumer stats fully non-blocking ([14ef43f](https://github.com/ayeshLK/immulog/commit/14ef43fe9b3a87ba8751410d1032e929a272671b))
* make consumer stats non-blocking ([9f0fe49](https://github.com/ayeshLK/immulog/commit/9f0fe498b18917d8cef7eb6b597b15fa09c64fb8))
* make consumer stats non-blocking ([b4175f5](https://github.com/ayeshLK/immulog/commit/b4175f5f552d5a6064987a1c9ede201337022ebf))
* protect consumer leases during admission ([47b033f](https://github.com/ayeshLK/immulog/commit/47b033f8eb2773ae196ff467f66b840dc4dc00b5))
* protect consumer leases during admission ([bb5f4f9](https://github.com/ayeshLK/immulog/commit/bb5f4f95d9605fa82749f5f4ce57bf2d2fdeba50))
* soak liveness regression from snapshot stall and unbounded descriptors ([74eae1f](https://github.com/ayeshLK/immulog/commit/74eae1fe33bfd3ed3331a0d5c8655c571a19d16f))
* stop soak liveness regression from snapshot stall and unbounded descriptors ([6ed3069](https://github.com/ayeshLK/immulog/commit/6ed306906f22a6dcc285a94ff3b25b3a01c73971))


### Performance Improvements

* add configurable soak runner ([0aec6e4](https://github.com/ayeshLK/immulog/commit/0aec6e4b8bbfe271ac430762cd37184b469563b4))
* add configurable soak runner ([20ff829](https://github.com/ayeshLK/immulog/commit/20ff829a16ad4dae61dc9a94385827b298287e8e))
* add reproducible benchmark runners ([ec3c3c9](https://github.com/ayeshLK/immulog/commit/ec3c3c9058b733c3018360c2a05cbca54e2d56a2))
* add reproducible benchmark runners ([9087a2e](https://github.com/ayeshLK/immulog/commit/9087a2e2f5def255f1bc52c1d9d8e9b1d59bde14))
* analyze and record benchmark results ([1cbdc6b](https://github.com/ayeshLK/immulog/commit/1cbdc6b250cfe3e10e17a2a88152dc9243dae31c))
* analyze and record benchmark results ([34aa128](https://github.com/ayeshLK/immulog/commit/34aa12839b41465dfe9eef46e4030b4ccc1ee946))
* estimate soak resource guardrails ([2fd319c](https://github.com/ayeshLK/immulog/commit/2fd319c005709075179783a325a9315b06b559f9))
* estimate soak resource guardrails ([d9da177](https://github.com/ayeshLK/immulog/commit/d9da177823a241e080273d628a20da22b1fa3e99))
* expand benchmark coverage ([5132e58](https://github.com/ayeshLK/immulog/commit/5132e5829e4cc77d47f76cf7322253e3ba407fe6))
* improve soak measurement evidence ([1d1d487](https://github.com/ayeshLK/immulog/commit/1d1d487af5ffcff10b562a5d861ed5e098de2432))
* improve soak measurement evidence ([04046ee](https://github.com/ayeshLK/immulog/commit/04046ee2f1ee66daf637434f28545baa8c865ebc))
* instrument soak consumer observability ([09a4090](https://github.com/ayeshLK/immulog/commit/09a40907bb33f5b9572ce50b23c7742ad56c1977))
* instrument soak consumer observability ([cbfd1d4](https://github.com/ayeshLK/immulog/commit/cbfd1d4bfd757e4a92719323edf5399b4843cb94))
* measure ingress linger behavior ([7ed288d](https://github.com/ayeshLK/immulog/commit/7ed288d78cb148fed9e25835b52171eef4651f7d))
* report consumer egress bytes ([a5aa35b](https://github.com/ayeshLK/immulog/commit/a5aa35b0cbefb31888148fc60abb16d3e794509c))
* report consumer egress bytes ([2b6193b](https://github.com/ayeshLK/immulog/commit/2b6193b95792fce56b353326ee53835c3672fdc5))
