@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title Gemini Link System

:: ========================================
:: 运行环境设置
:: ========================================
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

:: ========================================
:: 可按需修改的配置
:: ========================================
:: Python 解释器路径，留空则使用系统默认的 python
:: 示例：set "PYTHON_PATH=C:\Python311\python.exe"
set "PYTHON_PATH="

:: 后端与前端端口
set "BACKEND_PORT=4500"
set "FRONTEND_PORT=5000"

:: ========================================
:: 启动信息
:: ========================================
echo ========================================
echo   Gemini Link System 一键启动脚本
echo ========================================
echo 后端端口：%BACKEND_PORT%
echo 前端端口：%FRONTEND_PORT%
echo.

:: 组装 Python 命令
if "%PYTHON_PATH%"=="" (
    set "PYTHON_CMD=python"
) else (
    set "PYTHON_CMD=%PYTHON_PATH%"
)

:: 检查 Python 是否可用
"%PYTHON_CMD%" --version >nul 2>nul
if errorlevel 1 (
    echo [错误] 未找到 Python：%PYTHON_CMD%
    pause
    exit /b 1
)

:: 检查 Node.js 是否可用
where node >nul 2>nul
if errorlevel 1 (
    echo [错误] 未找到 Node.js，请先安装 Node.js 18+
    pause
    exit /b 1
)

:: 获取脚本所在目录
set "ROOT_DIR=%~dp0"

:: ========================================
:: 后端启动
:: ========================================
echo [1/4] 检查后端依赖...
cd /d "%ROOT_DIR%backend"

:: 首次运行时安装 Python 依赖
if not exist "__pycache__" (
    echo [提示] 首次运行，正在安装后端依赖...
    "%PYTHON_CMD%" -m pip install -r requirements.txt
)

echo [2/4] 启动后端服务...

:: 如果端口已被占用，则复用现有后端进程，避免重复启动导致 10048
call :get_port_pid %BACKEND_PORT% BACKEND_PID
if defined BACKEND_PID (
    echo [提示] 检测到后端已在运行，复用现有进程（PID !BACKEND_PID!）
) else (
    start "Gemini Backend" /b "%PYTHON_CMD%" -m uvicorn main:app --host 0.0.0.0 --port %BACKEND_PORT%
)

:: 等待后端完成初始启动
timeout /t 3 /nobreak >nul

:: ========================================
:: 前端启动
:: ========================================
echo [3/4] 检查前端依赖...
cd /d "%ROOT_DIR%frontend"

:: 首次运行时安装前端依赖
if not exist "node_modules" (
    echo [提示] 首次运行，正在安装前端依赖...
    call npm install
)

echo [4/4] 启动前端服务...

:: 如果前端已在运行，则直接复用并打开浏览器
call :get_port_pid %FRONTEND_PORT% FRONTEND_PID
if defined FRONTEND_PID (
    echo [提示] 检测到前端已在运行，复用现有进程（PID !FRONTEND_PID!）
    echo [提示] 如需完整重启，请先运行 stop.bat
    start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:%FRONTEND_PORT%"
    goto :done
)

:: 延迟打开浏览器，避免前端尚未监听时打开空白页
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:%FRONTEND_PORT%"

echo.
echo ========================================
echo   启动完成
echo ========================================
echo 后端地址： http://localhost:%BACKEND_PORT%
echo 前端地址： http://localhost:%FRONTEND_PORT%
echo API 文档： http://localhost:%BACKEND_PORT%/docs
echo.

:: 前台运行前端，便于直接观察日志
call npm run dev -- --port %FRONTEND_PORT%
goto :eof

:done
echo.
echo ========================================
echo   启动完成
echo ========================================
echo 后端地址： http://localhost:%BACKEND_PORT%
echo 前端地址： http://localhost:%FRONTEND_PORT%
echo API 文档： http://localhost:%BACKEND_PORT%/docs
echo.
goto :eof

:: 通过端口查找监听进程 PID
:get_port_pid
set "%~2="
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%~1" ^| findstr "LISTENING"') do (
    set "%~2=%%a"
    goto :eof
)
goto :eof