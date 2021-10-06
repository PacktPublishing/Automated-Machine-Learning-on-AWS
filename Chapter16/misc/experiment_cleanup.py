#!/usr/bin/env python3

# Basic Python script to clean up experiments
import boto3
import argparse
import time
from botocore.config import Config
from botocore.exceptions import ClientError

config = Config(retries = {'max_attempts': 10, 'mode': 'adaptive'})
sm = boto3.client('sagemaker', config=config)


def get_unassigned_trials():
    try:
        # Get the latest approved model package
        response = sm.list_trial_components(
            SortBy="CreationTime",
            MaxResults=100,
        )
        trials = response['TrialComponentSummaries']
        while len(trials) == 0 and "NextToken" in response:
            response = sm.list_trial_components(
                MaxResults=100,
                NextToken=response["NextToken"],
            )
            trials.extend(response['TrialComponentSummaries'])
        # Return error if no packages found
        if len(trials) == 0:
            error_message = (
                f"No Trials Found"
            )
            raise Exception(error_message)
        return trials
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        raise Exception(error_message)


def cleanup_assigned(experiment_name):
    response = sm.list_trials(ExperimentName=experiment_name, MaxResults=100)
    trials = response['TrialSummaries']
    while len(trials) == 0 and "NextToken" in trials:
        response = sm.list_trials(ExperimentName=experiment_name, MaxResults=100, NextToken=response["NextToken"])
        trials.extend(response['TrialSummaries'])
    if len(trials) == 0:
        error_message = ("No Trials Found")
        raise Exception(error_message)
    print('TrialNames:')
    for trial in trials:
        trial_name = trial['TrialName']
        print(f"\n{trial_name}")

        components_in_trial = sm.list_trial_components(TrialName=trial_name)
        print('\tTrialComponentNames:')
        for component in components_in_trial['TrialComponentSummaries']:
            component_name = component['TrialComponentName']
            print(f"\t{component_name}")
            sm.disassociate_trial_component(TrialComponentName=component_name, TrialName=trial_name)
            try:
                # comment out to keep trial components
                sm.delete_trial_component(TrialComponentName=component_name)
            except:
                # component is associated with another trial
                continue
            # to prevent throttling
            time.sleep(1)
        sm.delete_trial(TrialName=trial_name)
        time.sleep(1)
    sm.delete_experiment(ExperimentName=experiment_name)
    print(f"\nExperiment {experiment_name} deleted")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, default='AbaloneExperiments')
    args, _ = parser.parse_known_args()
    
    # Delete assigned experiments
    cleanup_assigned(args.name)
    
    # Delete unassigned experiments
    for trial in get_unassigned_trials():
        print(f'Deleteing TrialName: {trial["TrialComponentName"]}')
        sm.delete_trial_component(TrialComponentName=trial['TrialComponentName'])
        time.sleep(1)
        