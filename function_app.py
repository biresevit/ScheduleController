import logging
import os
import azure.functions as func

from schedule_controller.cloud_scheduler import CloudScheduler
from schedule_controller.hand_history_results import HandHistoryProcessor

app = func.FunctionApp()


@app.timer_trigger(schedule="%SchedulerSchedule%", arg_name="timer", run_on_startup=False, use_monitor=True)
def schedule_tick(timer: func.TimerRequest) -> None:
    if os.getenv("SCHEDULER_FUNCTION_ENABLED", "disabled").lower() == "disabled":
        logging.info("Scheduler Function is deployed but disabled")
        return
    results = CloudScheduler().tick_all()
    logging.info("Scheduler tick completed: %s", results)


@app.timer_trigger(schedule="%HandHistoryProcessorSchedule%", arg_name="timer", run_on_startup=False, use_monitor=True)
def hand_history_results_tick(timer: func.TimerRequest) -> None:
    if os.getenv("HAND_HISTORY_PROCESSOR_ENABLED", "disabled").lower() == "disabled":
        logging.info("Hand-history result processor is deployed but disabled")
        return
    result = HandHistoryProcessor().tick(limit=100)
    logging.info("Hand-history result processing completed: %s", result)
