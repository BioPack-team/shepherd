import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "shepherd_server.server:APP",
        host="0.0.0.0",
        port=5439,
        workers=4,
    )
