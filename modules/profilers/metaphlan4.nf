process run_metaphlan4 {
	
	input:
	tuple val(sample), path(fastq)
	path(mp4_db)

	output:
	tuple val(sample), path("mp4/${sample.id}/${sample.id}.mp4.txt"), emit: mp4_table
	
	script:
	def mp4_params = "--bowtie2db ${mp4_db} --input_type fastq --nproc ${task.cpus}"
	def mp4_input = ""
	def bt2_out = "--bowtie2out ${sample.id}.bowtie2.bz2"
	if (sample.is_paired) {
		mp4_input = "${fastq}"
	} else {
		mp4_input = "${sample.id}_R1.fastq.gz,${sample.id}_R2.fastq.gz"
	}

	"""
	mkdir -p mp4/${sample.id}/

	metaphlan ${mp4_input} ${mp4_params} ${bt2_out} -o mp4/${sample.id}/${sample.id}.mp4.txt
	"""


}