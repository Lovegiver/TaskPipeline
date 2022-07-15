package com.citizenweb.tooling.taskpipeline.core.model;

import com.citizenweb.tooling.taskpipeline.core.exceptions.TaskExecutionException;
import reactor.core.publisher.Flux;

/**
 * An {@link Operation} is a simple {@link FunctionalInterface}.<br>
 * Its single abstract method processes a varargs of {@link Flux} and return a {@link Flux}.<br>
 * <br>
 * Creating an Operation is simply a matter of organizing flux of data so that the whole process, dispatched among
 * many Operations all linked together, remain a unique Flux.<br>
 * Working in a reactive way seems to be a quite complex task. And it is.
 * Operation is a little object dedicated to make this easier in 3 steps :
 * <ol>
 *     <li>define as much Operation as possible. Operation should be a very simple object, doing only one thing, a
 *     kind of pure function</li>
 *     <li>encapsulate each {@link Operation} in a {@link Task}. A Task is just a wrapper for an Operation. We
 *     need an object like the Task, because it is a real Class, with properties. An Operation is just an interface.
 *     A Task stores values telling HOW tasks (so, Operations) interact with each other, if they are to be executed
 *     before or after another one.</li>
 *     <li>inject all {@link Task}s in a {@link Pipeline} that will just execute the whole process, deciding by itself
 *     in which order</li>
 * </ol>
 */
@FunctionalInterface
public interface Operation {
    /**
     * Processes the input {@link Flux} with any logic you deserve and then return a Flux.<br>
     *
     * @param inputs the Flux coming from preceding Operations
     * @return a Flux to be used by next Operation-s or to be subscribed to in order to finally get the result
     */
    Flux<?> process(Flux<?>... inputs) throws TaskExecutionException;
}
