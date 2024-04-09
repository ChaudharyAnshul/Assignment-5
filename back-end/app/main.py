from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth

app = FastAPI()

origins = [
    'http://localhost:8000',
]

#Handles https requests and responses
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# handeling authentication
app.include_router(auth.router, tags=['Auth'], prefix='/api/auth')

