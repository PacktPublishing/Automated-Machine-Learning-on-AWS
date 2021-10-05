from aws_cdk import core as cdk
from aws_cdk import aws_codecommit as codecommit
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import pipelines
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_iam as iam
from aws_cdk import aws_codebuild as codebuild
from ml_workflow_stack import MLWorkflowStack
from dev_applicaiton_stack import DevApplicationStack
from prod_applicaiton_stack import ProdApplicationStack


class MLWorkflowStage(cdk.Stage):
    def __init__(self, scope: cdk.Construct, id: str, *, group_name: str, threshold: float,**kwargs):
        super().__init__(scope, id, **kwargs)
        workflow_stack = MLWorkflowStack(self, 'MLWorkflowResources', group_name=group_name, threshold=threshold)
        self.sfn_arn = workflow_stack.sfn_output
        self.data_bucket = workflow_stack.data_bucket


class DevApplicaitonStage(cdk.Stage):
    def __init__(self, scope: cdk.Construct, id: str, *, group_name:str, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        dev_stack = DevApplicationStack(self, 'DevApplication', group_name=group_name, model_name=model_name)
        self.cdn_url = dev_stack.cdn_output
        self.api_url = dev_stack.httpapi_output


class ProdApplicaitonStage(cdk.Stage):
    def __init__(self, scope: cdk.Construct, id: str, *, group_name:str, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        prod_stack = ProdApplicationStack(self, 'ProdApplication', group_name=group_name, model_name=model_name)
        self.cdn_url = prod_stack.cdn_output
        self.api_url = prod_stack.httpapi_output


class PipelineStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, *, model_name: str, group_name: str, threshold: float, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Create a CodeCommit repository called 'mlops' as the source for the MLOps Pipeline
        mlops_repo = codecommit.Repository(
            self,
            'Repository',
            repository_name='mlops',
            description='MLOps Pipeline Repository.'
        )
        
        # Define Source Artifact representing the pipeline source code
        source_artifact = codepipeline.Artifact()
        
        # Define the artifact representing the cloud assembly
        cloud_assembly_artifact = codepipeline.Artifact()
        
        # Define the CDK Pipeline
        pipeline = pipelines.CdkPipeline(
            self,
            'CdkPipeline',
            cloud_assembly_artifact=cloud_assembly_artifact,
            cdk_cli_version='1.95.1', # Ensure cloud assemby uses a consistent CDK version   
            pipeline_name='MLOpsPipeline',
            
            # Define the source artifact from the 'mlops' repository
            source_action=codepipeline_actions.CodeCommitSourceAction(
                action_name='CodeCommit',
                repository=mlops_repo,
                output=source_artifact
            ),
            
            # Build the pipeline source code into a could assembly artifact
            synth_action=pipelines.SimpleSynthAction(
                source_artifact=source_artifact,
                cloud_assembly_artifact=cloud_assembly_artifact,
                install_command='printenv && npm install -g aws-cdk@1.95.1 && pip install -r requirements.txt',
                synth_command='cdk synth'
            )
        )
        
#         # Define the First Stage of the Application Pipeline: Building and Execute the ML Workflow
#         workflow = MLWorkflowStage(
#             self,
#             'BuildMLWorkflowStage',
#             group_name=group_name,
#             threshold=threshold
#         )
#         workflow_stage = pipeline.add_application_stage(workflow)
#         workflow_stage.add_actions(
#             pipelines.ShellScriptAction(
#                 action_name='MLWorkflow.Execute',
#                 run_order=workflow_stage.next_sequential_run_order(),
#                 additional_artifacts=[
#                     source_artifact
#                 ],
#                 commands=[
#                     'python3 ./scripts/invoke.py'
#                 ],
#                 use_outputs={
#                     'STATEMACHINE_ARN': pipeline.stack_output(workflow.sfn_arn),
#                     'DATA_BUCKET': pipeline.stack_output(workflow.data_bucket)
#                 },
#                 environment_variables={
#                     'MODEL_NAME': codebuild.BuildEnvironmentVariable(
#                         value=model_name,
#                         type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
#                     ),
#                     'PIPELINE_NAME': codebuild.BuildEnvironmentVariable(
#                         value='MLOpsPipeline',
#                         # value=pipeline.code_pipeline.pipeline_name,
#                         type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
#                     ),
#                     'STAGE_NAME': codebuild.BuildEnvironmentVariable(
#                         value='BuildMLWorkflowStage',
#                         type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
#                     ),
#                     'ACTION_NAME': codebuild.BuildEnvironmentVariable(
#                         value='MLWorkflow.Execute',
#                         type=codebuild.BuildEnvironmentVariableType.PLAINTEXT
#                     )
#                 },
#                 role_policy_statements=[
#                   iam.PolicyStatement(
#                       actions=[
#                           'states:ListStateMachines',
#                           'states:DescribeStateMachine',
#                           'states:DescribeExecution',
#                           'states:ListExecutions',
#                           'states:GetExecutionHistory',
#                           'states:StartExecution',
#                           'states:StopExecution'
#                       ],
#                       effect=iam.Effect.ALLOW,
#                       resources=['*']
#                   )
#                 ]
#             )
#         )
        
#         # Define the Second Stage of the Application Pipeline: Deploying the Application into a 'Dev/Test' environment
#         dev_application = DevApplicaitonStage(
#             self,
#             'DevDeploymentStage',
#             # env={
#             #     'account': DEV_ACCOUNT_ID,
#             #     'region': 'us-east-2'
#             # },
#             group_name=group_name,
#             model_name=model_name
#         )
#         dev_application_stage = pipeline.add_application_stage(dev_application)
#         dev_application_stage.add_actions(
#             pipelines.ShellScriptAction(
#                 action_name='DevApplication.IntegrationTests',
#                 run_order=dev_application_stage.next_sequential_run_order(),
#                 additional_artifacts=[
#                     source_artifact
#                 ],
#                 commands=[
#                     'pip install -r ./tests/requirements.txt',
#                     'pytest ./tests/integration_tests.py'
#                 ],
#                 use_outputs={
#                     'WEBSITE_URL': pipeline.stack_output(dev_application.cdn_url),
#                     'API_URL': pipeline.stack_output(dev_application.api_url)
#                 }
#             )
#         )

#         # Define the Third Stage of the Application Pipeline: Deploying the Application into the 'Production' environment
#         prod_application = ProdApplicaitonStage(
#             self,
#             'ProdDeploymentStage',
#             # env={
#             #     'account': PROD_ACCOUNT_ID,
#             #     'region': 'us-east-2'
#             # },
#             group_name=group_name,
#             model_name=model_name
#         )
#         prod_application_stage = pipeline.add_application_stage(prod_application)