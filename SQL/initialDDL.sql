
create table odm.ab_report(
	job_id bigint,
	job_name varchar(100) not null,
	start_time timestamp,
	complete_time timestamp,
	failed boolean,
	message text,
	env varchar(5),
	CONSTRAINT ab_report_pkey PRIMARY KEY (job_id,job_name)
)
DISTRIBUTED BY (job_id);

grant all on table odm.AB_Report to dev_edw_etl;



sudo mount //prodjobmgrgpv1/ASCI_ABATLOG/PRODABSCHEDV2/PRODJOBMGRGPV1 -t cifs /mnt/prod -o "username=hma"
sudo mount //testjobmgrgpv1/ASCI_ABATLOG/TESTACTBATCHV1/TESTJOBMGRGPV1 -t cifs /mnt/test -o "username=hma"