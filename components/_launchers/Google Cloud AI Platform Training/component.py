def run_task_on_cmle(
    task_spec,
    project_id: str,
    region: str,
    job_dir_prefix: str = None,
    scale_tier: str = None,
    master_type: str = None,
    worker_type: str = None,
    worker_count: int = None,
    additional_job_labels: dict = None,
    additional_master_config_parameters: dict = None,
    additional_worker_config_parameters: dict = None,
    additional_training_input_parameters: dict = None,
    additional_job_parameters: dict = None,
):
    import uuid
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
    from googleapiclient import errors

    from kfp.components._components import _resolve_command_line_and_paths
    
    component_spec = task_spec.component_ref.spec
    arguments = task_spec.arguments

    resolved_cmd = _resolve_command_line_and_paths(
        component_spec=component_spec,
        arguments=arguments,
    )
    
    job_id = 'job_' + str(uuid.uuid4()).replace('-', '_')
    job_dir = None
    if job_dir_prefix:
        job_dir = job_dir_prefix.strip('/') + '/' + job_id

    master_config = {
        'imageUri': component_spec.implementation.container.image,
    }
    if additional_master_config_parameters:
        master_config.update(additional_master_config_parameters)
    worker_config = {}
    if additional_worker_config_parameters:
        worker_config.update(additional_worker_config_parameters)
    training_input = {
        'region': region,
        #'command': resolved_cmd.command + resolved_cmd.args,  # Unknown name "command" at 'job.training_input': Cannot find field.".
        'args': resolved_cmd.command + resolved_cmd.args,  # TODO: Switch to command when it becomes available
        'jobDir': job_dir,
        'scaleTier': scale_tier,

        'masterType': master_type,
        'masterConfig': master_config,

        'workerCount': worker_count,
        'workerType': worker_type,
        'workerConfig': worker_config,
    }
    if additional_training_input_parameters:
        training_input.update(additional_training_input_parameters)
    job_labels = {  # https://cloud.google.com/ai-platform/training/docs/resource-labels
        'component': (component_spec.name or "")[0:63].lower().replace(' ', '-'), # The value can only contain lowercase letters, numeric characters, underscores and dashes. The value can be at most 63 characters long. International characters are allowed.
    }
    if additional_job_labels:
        job_labels.update(additional_job_labels)
    job = {
        'labels': job_labels,
        'trainingInput': training_input,
    }
    job['jobId'] = job_id
    if additional_job_parameters:
        job.update(additional_job_parameters)
    ml_client = discovery.build('ml','v1')
    ml_jobs = ml_proj.jobs()
    request = ml_jobs.create(
        parent = 'projects/{}'.format(project_id),
        body=job,
    )
    result = request.execute()
    return result
