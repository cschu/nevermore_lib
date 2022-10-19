process run_metaphlan4 {
	
	input:
	tuple val(sample), path(fastq)
	path(mp4_db)

	output:
	tuple val(sample), path("mp4/${sample.id}/${sample.id}.mp4.txt"), emit: mp4_table
	
	script:
	def mp4_params = "--bowtie2db \$(dirname \$(readlink ${mp4_db})) --input_type fastq --nproc ${task.cpus}"
	def mp4_input = (sample.is_paired) ? "${fastq}" : "${sample.id}_R1.fastq.gz,${sample.id}_R2.fastq.gz"

	"""
	mkdir -p mp4/${sample.id}/

	metaphlan ${mp4_input} ${mp4_params} -o mp4/${sample.id}/${sample.id}.mp4.txt
	"""


}