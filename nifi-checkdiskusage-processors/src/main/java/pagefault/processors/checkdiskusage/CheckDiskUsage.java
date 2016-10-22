/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pagefault.processors.checkdiskusage;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"health", "filesystem"})
@CapabilityDescription("Regularly checks disk utilization to invoke backpressure when filesystem is full")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CheckDiskUsage extends AbstractProcessor {

    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d+{1,2})%");
    private static long last_update = 0L;
    
    private static boolean flowfile_repository_disk_available = false;
    private static boolean content_repository_disk_available  = false;
    private NiFiProperties nifiProperties = null;
    
    public static final PropertyDescriptor INTERVAL = new PropertyDescriptor.Builder()
	.name("Interval")
        .description("Number of seconds between filesystem usage checks")
        .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
        .required(true)
        .defaultValue("30 secs")
        .build();
    public static final PropertyDescriptor FLOWFILE_REPOSITORY_FREE = new PropertyDescriptor.Builder()
	.name("Flow File Repository % Free")
        .description("Minimum % free before applying backpressure")
        .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
        .required(true)
        .defaultValue("20%")
        .build();
    public static final PropertyDescriptor CONTENT_REPOSITORY_FREE = new PropertyDescriptor.Builder()
	.name("Content Repository % Free")
        .description("Minimum % free before applying backpressure")
        .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
        .required(true)
        .defaultValue("20%")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();
    public static final Relationship REL_DISKUSAGE_FAILURE = new Relationship.Builder()
            .name("diskusage.failure")
            .description("This relationship is used when the disk usage exceeds the required free threshold")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        
        
        final String properties_file = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH);
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(properties_file, null);
        this.nifiProperties = properties;
        
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(INTERVAL);
        descriptors.add(FLOWFILE_REPOSITORY_FREE);
        descriptors.add(CONTENT_REPOSITORY_FREE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    public static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            /* do nothing */
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();

        if ( flowFile == null ) {
            return;
        }
        
        final Integer interval = context.getProperty(INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        long epoch = System.currentTimeMillis();

	if ((epoch - last_update) >= interval) {

            final Path flowFileRepositoryPath = nifiProperties.getFlowFileRepositoryPath();
            Map<String, Path> contentRepositoryPaths = nifiProperties.getContentRepositoryPaths();

            File flowFileRepositoryFile = flowFileRepositoryPath.toFile();

            String contentRepoFreeTresholdValue = context.getProperty(CONTENT_REPOSITORY_FREE).getValue();
            Matcher contentRepoMatcher = PERCENT_PATTERN.matcher(contentRepoFreeTresholdValue.trim());
            contentRepoMatcher.find();
            String contentRepoPercentageVal = contentRepoMatcher.group(1);
            int contentRepoFreeThreshold = Integer.parseInt(contentRepoPercentageVal);

            last_update = epoch;

            for (Map.Entry<String, Path> map : contentRepositoryPaths.entrySet()) {
                Path contentRepositoryPath = map.getValue();
                
                File contentRepositoryFile = contentRepositoryPath.toFile();

                long contentRepositoryTotalBytes = contentRepositoryFile.getTotalSpace();
                long contentRepositoryFreeBytes   = contentRepositoryFile.getFreeSpace();

                double contentRepositoryFreePercent = (double) contentRepositoryFreeBytes / (double) contentRepositoryTotalBytes * 100D;
                
                if (contentRepositoryFreePercent > contentRepoFreeThreshold) {
                    content_repository_disk_available = true;
                    break;
                } else {
                    content_repository_disk_available = false;
                }
            }
            
            long flowFileRepositoryTotalBytes = flowFileRepositoryFile.getTotalSpace();
            long flowFileRepositoryFreeBytes  = flowFileRepositoryFile.getFreeSpace();

            double flowFileRepositoryFreePercent = (double) flowFileRepositoryFreeBytes / (double) flowFileRepositoryTotalBytes * 100D;
            String flowfileRepoFreeTresholdValue = context.getProperty(FLOWFILE_REPOSITORY_FREE).getValue();
                
            Matcher flowFileRepoMatcher = PERCENT_PATTERN.matcher(flowfileRepoFreeTresholdValue.trim());
            flowFileRepoMatcher.find();
	    String flowFileRepoPercentageVal = flowFileRepoMatcher.group(1);
	    int flowFileRepoFreeThreshold = Integer.parseInt(flowFileRepoPercentageVal);

            flowfile_repository_disk_available = flowFileRepositoryFreePercent > flowFileRepoFreeThreshold;
        }

	if (flowfile_repository_disk_available == true && content_repository_disk_available == true) {
		session.transfer(flowFile, REL_SUCCESS);
	} else {
                session.rollback(false);
                CheckDiskUsage.sleepQuietly(1000L);
	}
    }
}
