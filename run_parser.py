import asyncio
from parse_all_sources import parse_all_sources

async def main():
    await parse_all_sources()

if __name__ == "__main__":
    asyncio.run(main()) 