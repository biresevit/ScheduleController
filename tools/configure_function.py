"""Configure the scheduler Function and its least-privilege SQL principal."""
from __future__ import annotations
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
AZ=ROOT.parent/"PokerBackoffice"/".azure-venv"/"Scripts"/"python.exe"
SUB="542a29f9-4770-41b9-939e-3b65068884b9"
RESOURCE=f"/subscriptions/{SUB}/resourceGroups/poker-backoffice/providers/Microsoft.Web/sites/poker-schedule-controller"


def token():
    result=subprocess.run([str(AZ),"-m","azure.cli","account","get-access-token","--subscription",SUB,"-o","json"],capture_output=True,text=True,check=True)
    return json.loads(result.stdout)["accessToken"]


def arm(method,path,body=None):
    request=urllib.request.Request("https://management.azure.com"+path+"?api-version=2024-11-01",
        data=None if body is None else json.dumps(body).encode(),method=method,
        headers={"Authorization":"Bearer "+token(),"Content-Type":"application/json"})
    with urllib.request.urlopen(request,timeout=90) as response:
        return json.load(response)


def main():
    sys.path.insert(0,str(ROOT))
    from schedule_controller.database import connection_string
    import pyodbc
    username="poker_scheduler_app"
    password=secrets.token_urlsafe(36)+"aA1!"
    conn=pyodbc.connect(connection_string(),autocommit=False,timeout=15)
    try:
        c=conn.cursor();c.execute("SET XACT_ABORT ON")
        exists=c.execute("SELECT 1 FROM sys.database_principals WHERE name=?",username).fetchone()
        quoted=password.replace("'","''")
        if exists: c.execute(f"ALTER USER [{username}] WITH PASSWORD='{quoted}'")
        else: c.execute(f"CREATE USER [{username}] WITH PASSWORD='{quoted}'")
        grants={
            "dbo.BotAccount":"SELECT","dbo.CustomerHandHistory":"SELECT",
            "reporting.HandProcessing":"SELECT,INSERT,UPDATE",
            "reporting.HandPlayerResult":"SELECT,INSERT,UPDATE,DELETE",
            "scheduler.Site":"SELECT,UPDATE","scheduler.StakeBand":"SELECT","scheduler.ScheduleWindow":"SELECT","scheduler.MttPlan":"SELECT",
            "scheduler.AccountPolicy":"SELECT","scheduler.AccountDay":"SELECT","scheduler.MachineAssignment":"SELECT",
            "scheduler.AccountDiagnosticSnapshot":"SELECT,INSERT,UPDATE",
            "scheduler.LobbyTable":"SELECT","scheduler.TableSeat":"SELECT","scheduler.AccountLease":"SELECT,INSERT,UPDATE,DELETE",
            "scheduler.AccountSession":"SELECT,INSERT,UPDATE","scheduler.Entry":"SELECT,INSERT,UPDATE",
            "scheduler.Command":"SELECT,INSERT,UPDATE","scheduler.Outbox":"SELECT,INSERT,UPDATE"}
        for table,rights in grants.items(): c.execute(f"GRANT {rights} ON OBJECT::{table} TO [{username}]")
        c.execute("GRANT EXECUTE ON OBJECT::scheduler.ExpireOfflineMachines TO [poker_scheduler_app]")
        conn.commit()
    except BaseException:
        conn.rollback();raise
    finally: conn.close()
    settings=arm("POST",RESOURCE+"/config/appsettings/list",{}).get("properties",{})
    settings.update({
        "SqlConnectionString":f"Server=tcp:pokerdbserver.database.windows.net,1433;Initial Catalog=pokerdb;User ID={username};Password={password};Encrypt=True;TrustServerCertificate=False;Connection Timeout=15;",
        "SchedulerSchedule":"*/30 * * * * *","SCHEDULER_FUNCTION_ENABLED":"disabled",
        "HandHistoryProcessorSchedule":"15,45 * * * * *","HAND_HISTORY_PROCESSOR_ENABLED":"disabled",
        "AzureWebJobsFeatureFlags":"EnableWorkerIndexing"})
    arm("PUT",RESOURCE+"/config/appsettings",{"properties":settings})
    print(json.dumps({"configured":True,"sql_user":username,"timer":"30 seconds","enabled":False}))


if __name__=="__main__": main()
