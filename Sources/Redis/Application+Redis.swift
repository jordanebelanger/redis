import Vapor

extension Application {
    public var redis: Redis {
        .init(application: self)
    }

    public struct Redis {
        private struct ConfigurationKey: StorageKey {
            typealias Value = RedisConfiguration
        }

        public var configuration: RedisConfiguration? {
            get {
                self.application.storage[ConfigurationKey.self]
            }
            nonmutating set {
                if self.application.storage.contains(ConnectionPoolKey.self) {
                    fatalError("Cannot modify your Redis configuration after redis has been used")
                }
                self.application.storage[ConfigurationKey.self] = newValue
            }
        }

        private struct ConnectionPoolKey: StorageKey, LockKey {
            typealias Value = EventLoopGroupConnectionPool<RedisConnectionSource>
        }

        public var connectionPool: EventLoopGroupConnectionPool<RedisConnectionSource> {
            if let existing = self.application.storage[ConnectionPoolKey.self] {
                return existing
            } else {
                let lock = self.application.locks.lock(for: ConnectionPoolKey.self)
                lock.lock()
                defer { lock.unlock() }
                guard let configuration = self.configuration else {
                    fatalError("Redis not configured. Use app.redis.configuration = ...")
                }
                let new = EventLoopGroupConnectionPool(
                    source: RedisConnectionSource(configuration: configuration, logger: self.application.logger),
                    maxConnectionsPerEventLoop: 1,
                    logger: self.application.logger,
                    on: self.application.eventLoopGroup
                )
                self.application.storage.set(ConnectionPoolKey.self, to: new) {
                    $0.shutdown()
                }
                return new
            }
        }
        
        let application: Application
    }
}

extension Application.Redis: RedisClient {
    public var logger: Logger? {
        self.application.logger
    }
    
    public var eventLoop: EventLoop {
        self.application.eventLoopGroup.next()
    }
    
    public func send(command: String, with arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        self.application.redis.connectionPool.withConnection(
            logger: logger,
            on: nil
        ) {
            $0.send(command: command, with: arguments)
        }
    }
    
}
