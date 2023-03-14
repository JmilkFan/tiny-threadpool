/* ********************************
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 *//** @file thpool.h *//*
 *
 ********************************/

#if defined(__APPLE__)
#include <AvailabilityMacros.h>
#else
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "thpool.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str) fprintf(stderr, str)
#else
#define err(str)
#endif

/**
 * 保持活跃标签。
 * 	1：所有线程永循环保持活跃。
 * 	0：所有线程执行完后休眠。
*/
static volatile int threads_keepalive;

/**
 * 暂停/恢复标签。
 * 	1（pause）：所有线程进入休眠；
 * 	0（resume）：所有线程恢复工作。
*/
static volatile int threads_on_hold;



/* ========================== STRUCTURES ============================ */


/**
 * 二元信号量，用于标识 Job Queue 是否为空。
 * 同时，应用条件变量的特性，也作为有 New Job 入队的触发器，触发 Thread 从 Job queue 中获取 Job。
 */
typedef struct bsem {
	pthread_mutex_t mutex;  // 信号量的锁
	pthread_cond_t   cond;  // 信号量的条件
	int v;                  // 信号量的互斥值
							//   0：空队列；
							//   1：非空队列。
} bsem;


/**
 * Job 应该包含以下 3 个成员：
 *  1. 线程入口函数；
 * 	2. 线程入口函数的参数；
 * 	3. 指向下一个 Job 的指针。
 */
typedef struct job{
	struct job*  prev;  // 指向下一个 Job，添加 New Job 入队尾（Rear）时，上一次的 Rear Job，应该要指向下一个 New Job，然后 New Job 成为新的 Near Job。
	void   (*function)(void* arg);  // 线程入口函数的类型声明
	void*  arg;                     // 线程入口函数的参数的类型声明
} job;


/**
 * Job Queue 应该包含以下 3 个成员：
 *  1. 队头
 * 	2. 队尾
 *  3. 队长
 *  4. 互斥锁，保证高并发 Jobs 入队/出队是 FIFO 的。
 * 	5. 队列状态：1）空队列；2）非空队列；
 */
typedef struct jobqueue{
	job  *front;              // 指向队头
	job  *rear;               // 指向队尾
	int   len;                // 队列长度
	pthread_mutex_t rwmutex;  // 任务队列的锁
	bsem *has_jobs;           // 指向一个二元信号量，用于表示 Queue 中是否有 Jobs。1 表示有；0 表示没有。
} jobqueue;


/**
 * Thread 应该包含以下 3 个成员：
 *  1. 友好的 ID，便于调试。区别于 Kernel 分配的 TID。
 * 	2. 指向 pthread 实体的指针
 *  3. 指向 Thread Pool 的指针，以此来获得/释放线程池的锁和条件变量
 */
typedef struct thread{
	int       id;              // 友好 ID
	pthread_t pthread;         // 指向一个线程实体
	struct thpool_* thpool_p;  // 指向线程池
} thread;


/**
 * Thread Pool Manager 应该包含以下 5 个成员：
 *  1. 多线程列表
 *  2. 活跃线程数量
 * 	3. 工作线程数量（可用线程数 = 活跃线程数量 - 工作线程数量）
 *  4. 任务队列
 *  5. 互斥锁
 *  6. 条件变量
 */
typedef struct thpool_{
	thread**   threads;                // 指向线程指针数组
	volatile int num_threads_alive;    // 当前活跃的线程数量
	volatile int num_threads_working;  // 当前工作中的线程数量
	jobqueue  jobqueue;                // 线程池关联的任务队列
	pthread_mutex_t  thcount_lock;     // 线程池的锁
	pthread_cond_t  threads_all_idle;  // 线程池的条件变量
} thpool_;





/* ========================== PROTOTYPES ============================ */


static int  thread_init(thpool_* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
static void  thread_hold(int sig_id);
static void  thread_destroy(struct thread* thread_p);

static int   jobqueue_init(jobqueue* jobqueue_p);
static void  jobqueue_clear(jobqueue* jobqueue_p);
static void  jobqueue_push(jobqueue* jobqueue_p, struct job* newjob_p);
static struct job* jobqueue_pull(jobqueue* jobqueue_p);
static void  jobqueue_destroy(jobqueue* jobqueue_p);

static void  bsem_init(struct bsem *bsem_p, int value);
static void  bsem_reset(struct bsem *bsem_p);
static void  bsem_post(struct bsem *bsem_p);
static void  bsem_post_all(struct bsem *bsem_p);
static void  bsem_wait(struct bsem *bsem_p);





/* ========================== THREADPOOL ============================ */


/* Initialise thread pool */
struct thpool_* thpool_init(int num_threads){

	threads_on_hold   = 0;  // 初始不休眠
	threads_keepalive = 1;	// 初始保持活跃

	if (num_threads < 0){
		num_threads = 0;
	}

	/* 创建 Thread pool */
	thpool_* thpool_p;
	thpool_p = (struct thpool_*)malloc(sizeof(struct thpool_));
	if (thpool_p == NULL){
		err("thpool_init(): Could not allocate memory for thread pool\n");
		return NULL;
	}
	thpool_p->num_threads_alive   = 0;  // 初始活跃线程数为 0
	thpool_p->num_threads_working = 0;  // 初始工作线程数为 0

	/* 初始化 Job queue */
	if (jobqueue_init(&thpool_p->jobqueue) == -1){
		err("thpool_init(): Could not allocate memory for job queue\n");
		free(thpool_p);
		return NULL;
	}

	/* 创建 Threads 并加入 Pool */
	thpool_p->threads = (struct thread**)malloc(num_threads * sizeof(struct thread *));
	if (thpool_p->threads == NULL){
		err("thpool_init(): Could not allocate memory for threads\n");
		jobqueue_destroy(&thpool_p->jobqueue);
		free(thpool_p);
		return NULL;
	}

	pthread_mutex_init(&(thpool_p->thcount_lock), NULL);   // 初始化线程池互斥锁
	pthread_cond_init(&thpool_p->threads_all_idle, NULL);  // 初始化线程池条件变量

	/* 逐一初始化 Threads */
	int n;
	for (n=0; n<num_threads; n++){
		thread_init(thpool_p, &thpool_p->threads[n], n);
#if THPOOL_DEBUG
			printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
#endif
	}

	/* 等待所有 Threads 初始化完成（活跃状态）。*/
	while (thpool_p->num_threads_alive != num_threads) {}

	return thpool_p;
}


/* Add work to the thread pool */
int thpool_add_work(thpool_* thpool_p, void (*function_p)(void*), void* arg_p){
	job* newjob;

	/* 创建一个新的 Job。*/
	newjob=(struct job*)malloc(sizeof(struct job));
	if (newjob==NULL){
		err("thpool_add_work(): Could not allocate memory for new job\n");
		return -1;
	}

	/* add function and argument */
	newjob->function=function_p;
	newjob->arg=arg_p;

	/* add job to queue */
	jobqueue_push(&thpool_p->jobqueue, newjob);

	return 0;
}


/* 开始等待，直到所有 Jobs 结束，Caller Thread 会被阻塞。*/
void thpool_wait(thpool_* thpool_p){
	pthread_mutex_lock(&thpool_p->thcount_lock);    // 获得线程池的锁
	/* 只要还有 Job 在 Queue 中，或者还有 Thread 在工作中，那么就会一直等待。*/
	while (thpool_p->jobqueue.len || thpool_p->num_threads_working) {
		pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);  // 阻塞 Caller Thread，直到有线程调用 pthread_cond_signal()。
	}
	pthread_mutex_unlock(&thpool_p->thcount_lock);  // 释放线程池的锁
}


/* 自下而上逐层销毁 Thread pool 的资源 */
void thpool_destroy(thpool_* thpool_p){
	/* No need to destroy if it's NULL */
	if (thpool_p == NULL) return ;

	volatile int threads_total = thpool_p->num_threads_alive;

	/* 结束每个线程的无限循环 */
	threads_keepalive = 0;

	/* 给一秒钟时间来终止空闲（idle）线程 */
	double TIMEOUT = 1.0;
	time_t start, end;
	double tpassed = 0.0;
	time (&start);
	while (tpassed < TIMEOUT && thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->jobqueue.has_jobs);
		time (&end);
		tpassed = difftime(end,start);
	}

	/* Poll remaining threads */
	while (thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->jobqueue.has_jobs);
		sleep(1);
	}

	/* Job queue cleanup */
	jobqueue_destroy(&thpool_p->jobqueue);
	/* Deallocs */
	int n;
	for (n=0; n < threads_total; n++){
		thread_destroy(thpool_p->threads[n]);
	}
	free(thpool_p->threads);
	free(thpool_p);
}


/* 用于暂停所有的线程，通过信号机制来实现 */
void thpool_pause(thpool_* thpool_p) {
	int n;
	for (n=0; n < thpool_p->num_threads_alive; n++){
		pthread_kill(thpool_p->threads[n]->pthread, SIGUSR1);  // 给所有工作线程发送 SIGUSR1，该信号的处理行为就是让线程休眠：thread_hold。
	}
}


/* Resume all threads in threadpool */
void thpool_resume(thpool_* thpool_p) {
    // resuming a single threadpool hasn't been
    // implemented yet, meanwhile this suppresses
    // the warnings
    (void)thpool_p;

	threads_on_hold = 0;
}


int thpool_num_threads_working(thpool_* thpool_p){
	return thpool_p->num_threads_working;
}





/* ============================ THREAD ============================== */


/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
static int thread_init (thpool_* thpool_p, struct thread** thread_p, int id){

	/* 创建新线程 */
	*thread_p = (struct thread*)malloc(sizeof(struct thread));
	if (*thread_p == NULL){
		err("thread_init(): Could not allocate memory for thread\n");
		return -1;
	}

	(*thread_p)->thpool_p = thpool_p;
	(*thread_p)->id       = id;  // 友好 ID，便于调试。

	/**
	 * 创建实际线程，并开始执行 thread_do（Worker 函数，在其内部执行 Job 函数）。
	 * 1、`(void * (*)(void *))` 用于将指针函数 `static void* thread_do(struct thread* thread_p)` 类型转换为函数指针 `void *(*start_routine) (void *)`
	 * 2、将线程设置为可分离状态，线程在退出时，系统可以自动回收线程资源，而无需通过调用 pthread_join 函数来进行回收。
	 */
	pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void *)) thread_do, (*thread_p));
	pthread_detach((*thread_p)->pthread);
	return 0;
}


/* Sets the calling thread on hold */
static void thread_hold(int sig_id) {
    (void)sig_id;
	threads_on_hold = 1;  // 永循环休眠，直到全局变量 threads_on_hold 被置 0。
	while (threads_on_hold){
		sleep(1);
	}
}


/** 
 * 初始化后就开启永循环的 pThread Worker 函数，在内部会调用 Job 函数。
 *
 * @param  thread        thread that will run this function
 * @return nothing
*/
static void* thread_do(struct thread* thread_p){

	/* Set thread name for profiling and debugging */
	char thread_name[16] = {0};
	snprintf(thread_name, 16, "thpool-%d", thread_p->id);

#if defined(__linux__)
	/* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
	prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
	pthread_setname_np(thread_name);
#else
	err("thread_do(): pthread_setname_np is not supported on this system");
#endif

	/* Assure all threads have been created before starting serving */
	thpool_* thpool_p = thread_p->thpool_p;

	/**
	 * 注册信号处理器（Signal handler），所有 pThread 都会在 thpool_pause 的时候接受到 SIGUSR1 信号。
	 * Handler thread_hold 的处理行为就是让 pThread 休眠。
	 */
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = thread_hold;
	if (sigaction(SIGUSR1, &act, NULL) == -1) {
		err("thread_do(): cannot handle SIGUSR1");
	}

	/* 加锁逐一更新 Thread pool 的活跃线程数值，以完成 Thread 的初始化工作。*/
	pthread_mutex_lock(&thpool_p->thcount_lock);    // 获得线程池的锁
	thpool_p->num_threads_alive += 1;               // 活跃线程数 +1
	pthread_mutex_unlock(&thpool_p->thcount_lock);  // 释放线程池的锁

	/* 进入永循环，直到全局变量 threads_keepalive 被置 0。*/
	while(threads_keepalive){

		/**
		 * 如果 Job queue 没有 Jobs，则在此处进入阻塞。
		 * 直到 New job 添加到 Queue，bsem_post 会发布 Has job 信号到此，继续执行下面的逻辑，获取 Job 并执行。
		*/
		bsem_wait(thpool_p->jobqueue.has_jobs);

		if (threads_keepalive){

			pthread_mutex_lock(&thpool_p->thcount_lock);    // 获得线程池的锁
			thpool_p->num_threads_working++;                // 工作线程数 +1
			pthread_mutex_unlock(&thpool_p->thcount_lock);  // 释放线程池的锁

			/* 从任务队列中弹出一个 Job 并开始执行。*/
			void (*func_buff)(void*);  // 定义一个线程入口函数的类型
			void*  arg_buff;           // 定义一个线程入口函数的参数的类型
			job* job_p = jobqueue_pull(&thpool_p->jobqueue);  // 从任务队列中取出队头的任务，初始时为 NULL
			if (job_p) {
				func_buff = job_p->function;
				arg_buff  = job_p->arg;
				func_buff(arg_buff);   // 开始执行任务
				free(job_p);
			}

			/* 已经完成 Job，或没有 Job，工作线程数 -1。*/
			pthread_mutex_lock(&thpool_p->thcount_lock);    // 获得线程池的锁
			thpool_p->num_threads_working--;                // 工作线程数 -1

			/* 没有任何工作线程了，向 thpool_wait 发送信号，等待所有 tasks 执行完。*/
			if (!thpool_p->num_threads_working) {
				pthread_cond_signal(&thpool_p->threads_all_idle);
			}
			
			pthread_mutex_unlock(&thpool_p->thcount_lock);  // 释放线程池的锁

		}
	}

	/* 全局变量 threads_keepalive 标记为 0，所有 Thread 执行完后陆续进入休眠。*/
	pthread_mutex_lock(&thpool_p->thcount_lock);    // 获得线程池的锁
	thpool_p->num_threads_alive --;                 // 活跃的线程 -1
	pthread_mutex_unlock(&thpool_p->thcount_lock);  // 释放线程池的锁

	return NULL;
}


/* Frees a thread  */
static void thread_destroy (thread* thread_p){
	free(thread_p);
}





/* ============================ JOB QUEUE =========================== */


/* Initialize queue */
static int jobqueue_init(jobqueue* jobqueue_p){
	jobqueue_p->len = 0;  // 初始队列长度为 0
	jobqueue_p->front = NULL;
	jobqueue_p->rear  = NULL;

	/* 创建 Job queue 的二元信号量 */
	jobqueue_p->has_jobs = (struct bsem*)malloc(sizeof(struct bsem));
	if (jobqueue_p->has_jobs == NULL){
		return -1;
	}

	pthread_mutex_init(&(jobqueue_p->rwmutex), NULL);  // 初始化任务队列的互斥锁
	bsem_init(jobqueue_p->has_jobs, 0);                // 初始化任务队列的二元信号量，0 表示队列为空。

	return 0;
}


/* Clear the queue */
static void jobqueue_clear(jobqueue* jobqueue_p){

	while(jobqueue_p->len){
		free(jobqueue_pull(jobqueue_p));
	}

	jobqueue_p->front = NULL;
	jobqueue_p->rear  = NULL;
	bsem_reset(jobqueue_p->has_jobs);
	jobqueue_p->len = 0;

}


/* Add (allocated) job to queue
 */
static void jobqueue_push(jobqueue* jobqueue_p, struct job* newjob){

	pthread_mutex_lock(&jobqueue_p->rwmutex);  // 获得任务队列的锁
	newjob->prev = NULL;  // New job 的下一个为空

	switch(jobqueue_p->len){

		case 0:  /* if no jobs in queue */
					jobqueue_p->front = newjob;
					jobqueue_p->rear  = newjob;
					break;

		default: /* if jobs in queue */
					jobqueue_p->rear->prev = newjob;  // 将上一个 Rear job 的下一个指向 New job。
					jobqueue_p->rear = newjob;        // 将 New job 入队尾，成为 Rear job。

	}
	jobqueue_p->len++;  // 队长 +1

	/* 添加了新的 Job，马上发布信号，给至少一个 Thread 接收到，并开始执行 New Job。*/
	bsem_post(jobqueue_p->has_jobs);
	pthread_mutex_unlock(&jobqueue_p->rwmutex);  // 释放任务队列的锁
}


/* Get first job from queue(removes it from queue)
 * Notice: Caller MUST hold a mutex
 */
static struct job* jobqueue_pull(jobqueue* jobqueue_p){

	pthread_mutex_lock(&jobqueue_p->rwmutex);  // 获得任务队列的锁
	job* job_p = jobqueue_p->front;  // 获得第一个出队的任务

	switch(jobqueue_p->len){

		case 0:  /* if no jobs in queue */
		  			break;

		case 1:  /* if one job in queue */
					jobqueue_p->front = NULL;
					jobqueue_p->rear  = NULL;
					jobqueue_p->len = 0;
					break;

		default: /* if >1 jobs in queue */
					jobqueue_p->front = job_p->prev;  // 将下一个任务移到队头
					jobqueue_p->len--;
					/* more than one job in queue -> post it */
					bsem_post(jobqueue_p->has_jobs);

	}

	pthread_mutex_unlock(&jobqueue_p->rwmutex);  // 释放任务队列的锁
	return job_p;
}


/* Free all queue resources back to the system */
static void jobqueue_destroy(jobqueue* jobqueue_p){
	jobqueue_clear(jobqueue_p);
	free(jobqueue_p->has_jobs);
}





/* ======================== SYNCHRONISATION ========================= */


/* Init semaphore to 1 or 0 */
static void bsem_init(bsem *bsem_p, int value) {
	if (value < 0 || value > 1) {
		err("bsem_init(): Binary semaphore can take only values 1 or 0");
		exit(1);
	}
	pthread_mutex_init(&(bsem_p->mutex), NULL);  // 初始化信号量的互斥锁
	pthread_cond_init(&(bsem_p->cond), NULL);    // 初始化信号量的条件变量
	bsem_p->v = value;  // 初始化信号量的数值，0 或 1
}


/* Reset semaphore to 0 */
static void bsem_reset(bsem *bsem_p) {
	bsem_init(bsem_p, 0);
}


/* 将队列不为空的信号最少发布到一个线程接收。*/
static void bsem_post(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);    // 获得信号量的锁
	bsem_p->v = 1;  // 二元值为 1，表示队列不为空
	pthread_cond_signal(&bsem_p->cond);    // 队列不为空时，向 bsem_wait 发送信号。
	pthread_mutex_unlock(&bsem_p->mutex);  // 释放信号量的锁
}


/* Post to all threads */
static void bsem_post_all(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	bsem_p->v = 1;
	pthread_cond_broadcast(&bsem_p->cond);
	pthread_mutex_unlock(&bsem_p->mutex);
}


/**
 * bsem_wait 在 thread_do 中被调用，用于在 Job queue 队列为空的时候阻塞 thread_do。
 * 当 bsem_post 调用 pthread_cond_signal() 激活 bsem_wait 后，表示至少有一个 thread_do 可以从 Job queue 中获取 Job 并执行了。
 * 
 * bsem_wait 会先将二元值归位为 0，然后继续运行 thread_do 的逻辑。
 */
static void bsem_wait(bsem* bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);    // 获得信号量的锁
	while (bsem_p->v != 1) {
		/* 阻塞当前线程，等待 bsem_post 调用 pthread_cond_signal()，然后重新获得锁，再将二元值置 0。*/
		pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
	}
	bsem_p->v = 0;  // 此时队列为空
	pthread_mutex_unlock(&bsem_p->mutex);  // 释放信号量的锁
}
