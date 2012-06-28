Huahin Unit is a unit test driver for Huahin MapReduce programs for use with JUnit and MRUnit.

Writing Huahin Unit Test cases

Huahin Unit testing framework is based on Junit and MRUnit and it can test
MapReduce programs written on 0.20.x, 0.22.x, 1.0.x version of Hadoop

-----------------------------------------------------------------------------
Install

Gets the jar using maven.

<dependency>
  <groupId>org.huahinframework</groupId>
  <artifactId>huahin-unit</artifactId>
  <version>x.x.x</version>
</dependency>

Or, get the source code.

git clone git://github.com/huahin/huahin-unit.git


-----------------------------------------------------------------------------
Test
All of Huahin Unit test run using the run method.

To test the Filter will inherit the FilterDriver. Set the Filter you want to test getFilter.
Parameters of the run is data that is used to input data and output.

The following example is Filter to get a tab delimited data.
And in the next test is to output the Record.

public class TestFilter extends Filter {
  @Override
  public void filter(Record record, Writer writer)
      throws IOException, InterruptedException {
    Record emitRecord = new Record();
    emitRecord.addGrouping("label", record.getValueString("label"));
    emitRecord.addValue("value", record.getValueString("value"));
    writer.write(emitRecord);
  }
  ...
}

public class FilterTest extends FilterDriver {
  private static final String LABELS = new String[] { "label", "value" };
  // first time filter test
  public void testString() {
     String input = "label\t1";

     Record output = new Record();
     output.addGrouping("label", "label");
     output.addValue("value", 1);

     run(LABELS, "\t", false, input, Arrays.asList(output));
  }

  // any time filter test
  public void testRecord() {
    Record input = new Record();
    input.addValue("label", "label");
    input.addValue("value", 1);

    Record output = new Record();
    output.addGrouping("label", "label");
    output.addValue("value", 1);

    run(input, Arrays.asList(output));
  }

  @Override
  public Filter getFilter() {
    return new TestFilter();
  }
}


To test the Summarizer will inherit the SummarizerDriver. Set the Summarizer you want to test getSummarizer.
Parameters of the run is data that is used to input data and output.
The following Summarizer test is count the number of labels.

public class TestSummarizer extends Summarizer {
  private int count;
  @Override
  public void init() {
    count = 0;
  }

  @Override
  public boolean summarizer(Record record, Writer writer)
      throws IOException, InterruptedException {
    count += record.getValueInteger(LABEL_VALUE);
    return false;
  }

  @Override
  public void end(Record record, Writer writer)
      throws IOException, InterruptedException {
    Record emitRecord = new Record();
    emitRecord.addValue(LABEL_VALUE, count);
    writer.write(emitRecord);
  }
}

public class SummarizerTest extends SummarizerDriver {
  public void test() {
    Record input1 = new Record();
    input1.addValue("label", "label");
    input1.addValue("value", 1);

    Record input2 = new Record();
    input2.addValue("label", "label");
    input2.addValue("value", 1);

    Record output = new Record();
    output.addGrouping("label", "label");
    output.addValue("value", 2);

    run(Arrays.asList(input1, input2), Arrays.asList(output));
  }

  public Summarizer getSummarizer() {
    return new TestSummarizer();
  }
}


To test the Job will inherit the JobDriver.
How to use the test is the same as that inherits Tool SimpleJobTool, to run will only use the run method.
Parameters of the run is data that is used to input data and output.
And the last parameter is whether to output the results actually executed.

public class JobTest extends JobDriver {
  private static final String LABELS = new String[] { "label", "value" };
  public void test()
    throws IOException, InstantiationException,
           IllegalAccessException, ClassNotFoundException {
    addJob(LABELS, StringUtil.TAB, false).setFilter(TestFilter.class)
                                         .setSummaizer(TestSummarizer.class);

    List<String> input = new ArrayList<String>();
    input.add("label\t1");
    input.add("label\t2");
    input.add("label\t3");

    List<Record> output = new ArrayList<Record>();
    Record record = new Record();
    record.addGrouping("label", "label");
    record.addValue("value", 6);
    output.add(record);

    run(input, output, true);
  }
}
