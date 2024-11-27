import asyncio

def do_stuff(arg):
    for i in range(arg):
        print(i)
    return
async def async_do_stuff(arg):
    do_stuff(arg)
    
async def main():
    await async_do_stuff(1)
    await async_do_stuff(2)
asyncio.run(main())