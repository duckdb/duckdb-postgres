@echo off
set FAKE_TOKEN=fake-rds-token-1234567890abcdef
if defined AWS_STUB_LOG (
    echo %* >> %AWS_STUB_LOG%
)
echo %FAKE_TOKEN%
