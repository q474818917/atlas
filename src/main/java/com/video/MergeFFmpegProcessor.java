package com.video;

import net.bramp.ffmpeg.builder.FFmpegBuilder;

public class MergeFFmpegProcessor extends CommonFFmpeg {
	
	private String firstInput;
	private String secondInput;
	private String output;
	
	public MergeFFmpegProcessor(String firstInput, String secondInput, 
			String output) {
		this.firstInput = firstInput;
		this.secondInput = secondInput;
		this.output = output;
	}
	
	@Override
	FFmpegBuilder builder() {
		return new FFmpegBuilder().
			addInput(firstInput).
			addInput(secondInput).
			addOutput(output).
			addExtraArgs("-c:v", "copy").
			addExtraArgs("-c:a", "aac").
			setStrict(FFmpegBuilder.Strict.EXPERIMENTAL).done();
	}
	
}
