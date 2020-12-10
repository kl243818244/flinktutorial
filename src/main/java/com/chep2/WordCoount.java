package com.chep2;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCoount {

	/**
	 * lambda 表达式的使用方式
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

		String path = "D:\\STS\\STS4_WORKSPACES\\flinktutorial\\src\\main\\resources\\hello.txt";

//		DataSource<String> readTextFile = executionEnvironment.readTextFile(path);
		DataSource<String> readTextFile = executionEnvironment.fromElements(WORDS);

		FlatMapOperator<String, String> flatMap = readTextFile
				.flatMap((String a, Collector<String> out) -> Arrays.stream(a.split(" ")).forEach(x -> out.collect(x)))
				.returns(Types.STRING);

		MapOperator<String, Tuple2<String, Integer>> map = flatMap.map(str -> new Tuple2<String, Integer>(str, 1))
				.returns(Types.TUPLE(Types.STRING, Types.INT));
		;

		AggregateOperator<Tuple2<String, Integer>> sum = map.groupBy(0).sum(1);

		sum.print();
	}

	private static final String[] WORDS = new String[] { "To be, or not to be,--that is the question:--",
			"Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
			"Or to take arms against a sea of troubles,", "And by opposing end them?--To die,--to sleep,--",
			"No more; and by a sleep to say we end", "The heartache, and the thousand natural shocks",
			"That flesh is heir to,--'tis a consummation", "Devoutly to be wish'd. To die,--to sleep;--",
			"To sleep! perchance to dream:--ay, there's the rub;", "For in that sleep of death what dreams may come,",
			"When we have shuffled off this mortal coil,", "Must give us pause: there's the respect",
			"That makes calamity of so long life;", "For who would bear the whips and scorns of time,",
			"The oppressor's wrong, the proud man's contumely,", "The pangs of despis'd love, the law's delay,",
			"The insolence of office, and the spurns", "That patient merit of the unworthy takes,",
			"When he himself might his quietus make", "With a bare bodkin? who would these fardels bear,",
			"To grunt and sweat under a weary life,", "But that the dread of something after death,--",
			"The undiscover'd country, from whose bourn", "No traveller returns,--puzzles the will,",
			"And makes us rather bear those ills we have", "Than fly to others that we know not of?",
			"Thus conscience does make cowards of us all;", "And thus the native hue of resolution",
			"Is sicklied o'er with the pale cast of thought;", "And enterprises of great pith and moment,",
			"With this regard, their currents turn awry,", "And lose the name of action.--Soft you now!",
			"The fair Ophelia!--Nymph, in thy orisons", "Be all my sins remember'd." };

}
