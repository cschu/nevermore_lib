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
	if (!sample.is_paired) {
		mp4_input = "${fastq}"
	} else {
		mp4_input = "${sample.id}_R1.fastq.gz,${sample.id}_R2.fastq.gz"
	}

	"""
	mkdir -p mp4/${sample.id}/

	metaphlan ${mp4_input} ${mp4_params} ${bt2_out} -o mp4/${sample.id}/${sample.id}.mp4.txt
	"""
}

process collate_metaphlan4_tables {

	input:
	tuple val(sample_id), path(tables)

	output:
	tuple val(sample_id), path("metaphlan4/${sample_id}.mp4_abundance_table.txt")

	script:
	"""
	mkdir -p metaphlan4/

	merge_metaphlan_tables.py ${tables} > metaphlan4/${sample_id}.mp4_abundance_table.txt
	"""

}