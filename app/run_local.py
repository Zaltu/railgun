import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:railgun_app",
        port=8889,
        host='localhost',  # DevEnv
        log_level="debug"
    )
