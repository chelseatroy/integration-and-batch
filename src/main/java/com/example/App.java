package com.example;

import com.sun.javafx.binding.Logging;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.mail.Mail;
import org.springframework.integration.dsl.mail.MailSendingMessageHandlerSpec;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;
import org.springframework.messaging.Message;

import java.io.File;
import java.util.Properties;

@SpringBootApplication
@EnableBatchProcessing
public class App {
    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private StepBuilderFactory steps;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    Properties mailProperties() {
        Properties properties = new Properties();
        properties.put("mail.smtp.ssl.enable", true);
        return properties;
    }

    @Bean
    IntegrationFlow integrationFlow() {
        return IntegrationFlows
                .from(Files.inboundAdapter(new File("build/input")),
                        c -> c.poller(Pollers.fixedDelay(3000)))
                .transform(fileMessageToJobRequest())
                .handle(new JobLaunchingGateway(jobLauncher))
                .transform(source ->  {
                    JobExecution source1 = (JobExecution) source;
                    return source1.getExitStatus().toString();
                })
                .enrichHeaders(headerEnricherSpec -> headerEnricherSpec.header("mail_to", "jcarroll@pivotal.io"))
                .handle(Mail.outboundAdapter("smtp.gmail.com")
                        .credentials("himom84567890@gmail.com", "GC8hC}T(K=P5[zCd")
                        .javaMailProperties(mailProperties()))
                .get();
    }

    FileMessageToJobRequest fileMessageToJobRequest() {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        fileMessageToJobRequest.setJob(job());

        return fileMessageToJobRequest;
    }

    private Job job() {
        return jobs
                .get("jobName")
                .start(step())
                .build();
    }

    private Step step() {
        return steps
                .get("stepName")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("contribution = [" + contribution + "], chunkContext = [" + chunkContext + "]");
                    return null;
                })
                .build();
    }

    public class FileMessageToJobRequest {
        private Job job;
        private String fileParameterName;

        public void setFileParameterName(String fileParameterName) {
            this.fileParameterName = fileParameterName;
        }

        public void setJob(Job job) {
            this.job = job;
        }

        @Transformer
        public JobLaunchRequest toRequest(Message<File> message) {
            JobParametersBuilder jobParametersBuilder =
                    new JobParametersBuilder();

            jobParametersBuilder.addString(fileParameterName,
                    message.getPayload().getAbsolutePath());

            return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
        }
    }
}
