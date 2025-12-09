"""
Example usage of AsyncRedisService with async/await.

This example demonstrates:
- Connecting to Redis asynchronously
- Setting and getting typed values
- Concurrent operations
- Proper connection lifecycle management
"""

import asyncio

from pydantic import BaseModel

from cledar.redis import AsyncRedisService, RedisServiceConfig


class UserModel(BaseModel):
    user_id: int
    name: str
    email: str


async def basic_usage_example() -> None:
    """Basic async Redis operations."""
    print("=== Basic Async Usage ===")

    # Configure service
    config = RedisServiceConfig(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
    )

    # Create and connect
    service = AsyncRedisService(config)
    await service.connect()

    try:
        # Health check
        is_alive = await service.is_alive()
        print(f"Redis is alive: {is_alive}")

        # Store typed data
        user = UserModel(user_id=1, name="Alice", email="alice@example.com")
        await service.set("user:1", user)
        print(f"Stored user: {user}")

        # Retrieve and validate
        retrieved = await service.get("user:1", UserModel)
        print(f"Retrieved user: {retrieved}")

        # Store raw string
        await service.set("greeting", "Hello, async world!")
        greeting = await service.get_raw("greeting")
        print(f"Greeting: {greeting}")

    finally:
        # Always close connection
        await service.close()
        print("Connection closed")


async def concurrent_operations_example() -> None:
    """Demonstrate concurrent async operations."""
    print("\n=== Concurrent Operations ===")

    config = RedisServiceConfig(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
    )

    service = AsyncRedisService(config)
    await service.connect()

    try:
        # Create multiple users concurrently
        users = [
            UserModel(user_id=i, name=f"User{i}", email=f"user{i}@example.com")
            for i in range(1, 11)
        ]

        # Store all users concurrently
        set_tasks = [service.set(f"user:{u.user_id}", u) for u in users]
        results = await asyncio.gather(*set_tasks)
        print(f"Stored {sum(results)} users concurrently")

        # Retrieve all users concurrently
        keys = [f"user:{i}" for i in range(1, 11)]
        retrieved = await service.mget(keys, UserModel)
        user_count = len([r for r in retrieved if isinstance(r, UserModel)])
        print(f"Retrieved {user_count} users")

        # Clean up concurrently
        delete_tasks = [service.delete(key) for key in keys]
        await asyncio.gather(*delete_tasks)
        print("Cleaned up all users")

    finally:
        await service.close()


async def main() -> None:
    """Run all examples."""
    await basic_usage_example()
    await concurrent_operations_example()


if __name__ == "__main__":
    asyncio.run(main())
