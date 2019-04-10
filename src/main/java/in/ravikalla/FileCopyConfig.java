package in.ravikalla;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.annotation.Filter;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.router.AbstractMessageRouter;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import in.ravikalla.model.Payor;
import in.ravikalla.util.Util;

@Configuration
@EnableIntegration
public class FileCopyConfig {

    public final String INPUT_DIR = "source";
    public final String OUTPUT_DIR = "target";
    public final String FILE_PATTERN = "*.txt";

    @Bean
    public MessageChannel fileChannel() {
        return new DirectChannel();
    }

    @Bean
    @InboundChannelAdapter(value = "payorFileSource", poller = @Poller(fixedDelay = "10000"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource sourceReader = new FileReadingMessageSource();
        sourceReader.setDirectory(new File(INPUT_DIR));
        sourceReader.setFilter(new SimplePatternFileListFilter(FILE_PATTERN));
        return sourceReader;
    }

    @Bean
    @Transformer(inputChannel="payorFileSource", outputChannel="payorFileContent")
    public FileToStringTransformer transformFileToString() {
    	FileToStringTransformer objFileToStringTransformer = new FileToStringTransformer();
    	return objFileToStringTransformer;
    }

//	@Bean
//    @ServiceActivator(inputChannel = "fileChannel", outputChannel="payorRawStringChannel")
//    public MessageHandler fileWritingMessageHandler() {
//        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File(OUTPUT_DIR));
//        handler.setFileExistsMode(FileExistsMode.REPLACE);
////        handler.setDeleteSourceFiles(true);
//        handler.setExpectReply(false);
//
//        return handler;
//    }

    @Splitter(inputChannel="payorFileContent", outputChannel="payorRawStringChannel")
    public List<String> splitFileContentToLines(String strFileContent) {
    	List<String> lstPayors = new ArrayList<String>();
    	Scanner scanner = new Scanner(strFileContent);
    	String line;
    	while (scanner.hasNextLine()) {
    	  line = scanner.nextLine();
    	  lstPayors.add(line);
    	}
    	scanner.close();

    	return lstPayors;
    }

    @Bean
    @Transformer(inputChannel="payorRawStringChannel", outputChannel="payorRawObjectChannel")
    public GenericTransformer<String, Payor> transformPayorStringToObject() {
    	GenericTransformer<String, Payor> genericTransformer = new GenericTransformer<String, Payor>() {
    		@Override
    		public Payor transform(String strPayor) {
    			String[] arrPayorData = strPayor.split(",");
    			Payor objPayor;
    			if (null != arrPayorData && arrPayorData.length > 1)
    				objPayor = new Payor(Integer.parseInt(arrPayorData[0]), arrPayorData[1]);
    			else
    				objPayor = new Payor();
    			return objPayor;
    		}
    	};
    	return genericTransformer;
    }

    @Filter(inputChannel="payorRawObjectChannel", outputChannel="filteredPayorChannel")
    public boolean filterBlankPayor(Payor objPayor) {
    	if (null != objPayor.getName() && !objPayor.getName().isEmpty())
    		return true;
    	else
    		return false;
    }

    @Bean
    @Router(inputChannel = "filteredPayorChannel")
    public AbstractMessageRouter validInvalidRoute() {
    	return new AbstractMessageRouter() {
			@Override
			protected Collection<MessageChannel> determineTargetChannels(Message<?> message) {
				Payor objPayor = (Payor) message.getPayload();
				if (Util.isValid(objPayor))
					return Collections.singleton(validChannel());
				else
					return Collections.singleton(invalidChannel());
			}
		};
    }

    @Bean
    public MessageChannel validChannel() {
    	return new DirectChannel();
    }

    @Bean
    public MessageChannel invalidChannel() {
    	return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel="validChannel")
    public MessageHandler sysoutValidHandler() {
    	return message -> {
    		Payor objPayor = (Payor) message.getPayload();
    		System.out.println("109 : Success Data : " + objPayor.getId() + " : " + objPayor.getName());
    	};
    }
    @Bean
    @ServiceActivator(inputChannel="invalidChannel")
    public MessageHandler sysoutInvalidHandler() {
    	return message -> {
    		Payor objPayor = (Payor) message.getPayload();
    		System.out.println("117 : Bad Data : " + objPayor.getId() + " : " + objPayor.getName());
    	};
    }

    public static void main(final String... args) {
        final AbstractApplicationContext context = new AnnotationConfigApplicationContext(FileCopyConfig.class);
        context.registerShutdownHook();
        final Scanner scanner = new Scanner(System.in);
        System.out.print("Please enter a string and press <enter>: ");
        while (true) {
            final String input = scanner.nextLine();
            if ("q".equals(input.trim())) {
                context.close();
                scanner.close();
                break;
            }
        }
        System.exit(0);
    }

}
    


