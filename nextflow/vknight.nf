#!/usr/bin/env nextflow

nextflow.enable.dsl=2


if (!params.publish_mode) {
	params.publish_mode = "symlink"
}

if (!params.output_dir) {
	params.output_dir = "vknight_out"
}

output_dir = "${params.output_dir}"

if (params.motus_database) {
	motus_database = "-db ${params.motus_database}"
} else {
	motus_database = ""
}

if (!params.pathseq_min_clipped_read_length) {
	params.pathseq_min_clipped_read_length = 31
}

def run_kraken2 = (!params.skip_kraken2 || params.run_kraken2);
def run_mtags = (!params.skip_mtags || params.run_mtags);
def run_motus2 = (!params.skip_motus2 || params.run_motus2);
def run_pathseq = (!params.skip_pathseq || params.run_pathseq);
def run_count_reads = (!params.skip_counts || params.run_counts);

def convert_fastq2bam = (run_pathseq || run_count_reads);





process bam2fq {
	input:
	tuple val(sample), path(bam)

	output:
	stdout
	tuple val(sample), path("out/${sample}*.fastq.gz"), emit: reads

	script:
	"""
	mkdir -p out
	samtools collate -@ $task.cpus -u -O $bam | samtools fastq -F 3840 -0 ${sample}_other.fastq.gz -1 ${sample}_R1.fastq.gz -2 ${sample}_R2.fastq.gz

	if [[ -z "\$(gzip -dc ${sample}_R1.fastq.gz | head -n 1)" ]];
	then
		mv ${sample}_other.fastq.gz out/${sample}_R1.fastq.gz;
	else
		mv ${sample}_R1.fastq.gz out/;
		if [[ ! -z "\$(gzip -dc ${sample}_R2.fastq.gz | head -n 1)" ]];
		then
			mv ${sample}_R2.fastq.gz out/;
		fi;
	fi;
	rm -rf *.fastq.gz
	"""
}


process prepare_fastqs {
	input:
	tuple val(sample), path(fq)

	output:
	tuple val(sample), path("out/${sample}_R*.fastq.gz") ,emit: reads

	script:
	if (fq.size() == 2) {
		"""
		mkdir -p out
		ln -sf ../${fq[0]} out/${sample}_R1.fastq.gz
		ln -sf ../${fq[1]} out/${sample}_R2.fastq.gz
		"""
	} else {
		"""
		mkdir -p out
		ln -sf ../${fq[0]} out/${sample}_R1.fastq.gz
		"""
	}
}


process fq2bam {
	input:
	tuple val(sample), path(fq)

	output:
	stdout
	tuple val(sample), path("out/${sample}.bam"), emit: reads

	script:

	if (fq.size() == 2) {
		"""
		mkdir -p out
		gatk FastqToSam -F1 ${fq[0]} -F2 ${fq[1]} -O out/${sample}.bam -SM $sample
		"""
	} else {
		"""
		mkdir -p out
		gatk FastqToSam -F1 ${fq[0]} -O out/${sample}.bam -SM $sample
		"""
	}
}


process count_reads {
	publishDir "$output_dir", mode: params.publish_mode

	input:
	tuple val(sample), path(bam)

	output:
	//tuple val(sample),
	path("${sample}/${sample}.libsize.txt"), emit: counts
	path("${sample}/${sample}.is_paired.txt"), emit: is_paired
	path("${sample}/${sample}.flagstats.txt"), emit: flagstats

	script:
	"""
	mkdir -p ${sample}
	samtools flagstat $bam > "${sample}/${sample}.flagstats.txt"
	head -n 1 "${sample}/${sample}.flagstats.txt" | awk '{print \$1 + \$3}' > "${sample}/${sample}.libsize.txt"
	grep -m 1 "paired in sequencing" "${sample}/${sample}.flagstats.txt" | awk '{npaired = \$1 + \$3; if (npaired==0) {print "unpaired"} else {print "paired"};}' > "${sample}/${sample}.is_paired.txt"
	"""
}


process kraken2 {
	publishDir "$output_dir", mode: params.publish_mode

	input:
	tuple val(sample), path(reads)

	output:
	tuple val(sample), path("${sample}/${sample}.kraken2_report.txt"), emit: kraken2_out

	script:
	def is_paired = (reads.size() == 2) ? "--paired" : "";
	"""
	mkdir -p ${sample}
	kraken2 --db ${params.kraken_database} --threads $task.cpus --gzip-compressed --report ${sample}/${sample}.kraken2_report.txt ${is_paired} \$(ls $reads)
	"""
}


process mtags_extract {
	input:
	tuple val(sample), path(reads)

	output:
	tuple val(sample), path("*_bac_ssu.fasta"), emit: mtags_out

	script:
	def mtags_input = (reads.size() == 2) ? "-f ${sample}_R1.fastq.gz -r ${sample}_R2.fastq.gz" : "-s ${sample}_R1.fastq.gz";

	"""
	mtags extract -t $task.cpus -o . ${mtags_input}
	"""
}


process mtags_annotate {
	input:
	tuple val(sample), path(seqs)

	output:
	path("${sample}.bins"), emit: mtags_bins

	script:
	def mtags_input = (seqs.size() == 2) ? "-f ${sample}_R1.fastq.gz_bac_ssu.fasta -r ${sample}_R2.fastq.gz_bac_ssu.fasta" : "-s ${sample}_R1.fastq.gz_bac_ssu.fasta";
	"""
	mtags annotate -t $task.cpus -o . -n ${sample} ${mtags_input}
	"""
}


process mtags_merge {
	publishDir "$output_dir", mode: params.publish_mode

	input:
	path(mtags_bins)

	output:
	path("mtags_tables/merged_profile.*"), emit: mtags_tables

	script:
	"""
	mkdir mtags_tables
	mtags merge -i *bins -o mtags_tables/merged_profile
	"""
}


process motus2 {
	publishDir "$output_dir", mode: params.publish_mode

	input:
	tuple val(sample), path(reads)

	output:
	tuple val(sample), path("${sample}/${sample}.motus.txt"), emit: motus_out

	script:
	def motus_input = (reads.size() == 2) ? "-f ${sample}_R1.fastq.gz -r ${sample}_R2.fastq.gz" : "-s ${sample}_R1.fastq.gz";
	"""
	mkdir -p ${sample}
	motus profile -t $task.cpus -g 1 -k genus -c -v 7 ${motus_database} ${motus_input} > ${sample}/${sample}.motus.txt
	"""
}


process pathseq {
	publishDir "$output_dir", mode: params.publish_mode

	input:
	tuple val(sample), path(bam)

	output:
	val(sample), emit: sample
	path("${sample}/${sample}.pathseq.bam"), emit: bam
	path("${sample}/${sample}.pathseq.bam.sbi"), emit: sbi
	path("${sample}/${sample}.pathseq.score_metrics"), emit: score_metrics
	path("${sample}/${sample}.pathseq.scores"), emit: txt

	script:
	"""
	mkdir -p ${sample}
	maxmem=\$(echo \"$task.memory\"| sed 's/ GB/g/g')
	gatk --java-options \"-Xmx\$maxmem\" PathSeqPipelineSpark \\
		--input $bam \\
		--filter-bwa-image ${params.pathseq_database}/reference.fasta.img \\
		--kmer-file ${params.pathseq_database}/host.hss \\
		--min-clipped-read-length ${params.pathseq_min_clipped_read_length} \\
		--microbe-fasta ${params.pathseq_database}/microbe.fasta \\
		--microbe-bwa-image ${params.pathseq_database}/microbe.fasta.img \\
		--taxonomy-file ${params.pathseq_database}/microbe.db \\
		--output ${sample}/${sample}.pathseq.bam \\
		--scores-output ${sample}/${sample}.pathseq.scores \\
		--score-metrics ${sample}/${sample}.pathseq.score_metrics
	"""
}


workflow {

	/*
		Collect all fastq files from the input directory tree.
		It is recommended to use one subdirectory per sample.
	*/

	fastq_ch = Channel
		.fromPath(params.input_dir + "/" + "**.{fastq,fq,fastq.gz,fq.gz}")
		.map { file ->
				def sample = file.name.replaceAll(/.(fastq|fq)(.gz)?$/, "")
				sample = sample.replaceAll(/_R?[12]$/, "")
				return tuple(sample, file)
		}
		.groupTuple(sort: true)

	/*
		Normalise the input fastq naming scheme
		r1: <sample>_R1.fastq.gz
		r2: <sample>_R2.fastq.gz
	*/

	prepare_fastqs(fastq_ch)

	/*
		Collect all bam files from the input directory tree.
	*/

	bam_ch = Channel
		.fromPath(params.input_dir + "/" + "**.bam")
		.map { file ->
			def sample = file.name.replaceAll(/.bam$/, "")
			return tuple(sample, file)
		}
		.groupTuple(sort: true)

	/*
		Convert input bam to fastq.
		This gets rid of:
		- alignment information (e.g. when reads were prealigned against host)
		- optical duplicates, secondary/suppl alignments, reads that don't pass qual filters
	*/

	bam2fq(bam_ch)

	/*
		Combine the normalised and bam-extracted fastqs
	*/

	combined_fastq_ch = prepare_fastqs.out.reads.concat(bam2fq.out.reads)

	if (convert_fastq2bam) {

		/*
			Convert all fastqs to bam files
		*/

		fq2bam(combined_fastq_ch)

		combined_bam_ch = fq2bam.out.reads

		/* perform bam-based analyses */

		if (run_count_reads) {
			count_reads(combined_bam_ch)
		}

		if (run_pathseq) {
			pathseq(combined_bam_ch)
		}

	}

	/* perform fastq-based analyses */

	if (run_kraken2) {
		kraken2(combined_fastq_ch)
	}

	if (run_motus2) {
		motus2(combined_fastq_ch)
	}

	if (run_mtags) {
		mtags_extract(combined_fastq_ch)

		mtags_annotate(mtags_extract.out.mtags_out)

		mtags_merge(mtags_annotate.out.mtags_bins.collect())
	}
}
