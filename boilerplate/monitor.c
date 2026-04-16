#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/sched/signal.h>
#include <linux/mm.h>
#include <linux/delay.h>
#include <linux/kthread.h>
#include <linux/ioctl.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"

MODULE_LICENSE("GPL");

struct container_entry {
    pid_t pid;
    char id[32];
    unsigned long soft;
    unsigned long hard;
    int soft_triggered;
    struct list_head list;
};

static LIST_HEAD(container_list);
static DEFINE_MUTEX(container_lock);

static struct task_struct *monitor_thread;

/* -------- MEMORY FUNCTION -------- */
static unsigned long get_rss(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    unsigned long rss = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return 0;
    }

    mm = get_task_mm(task);
    rcu_read_unlock();

    if (!mm)
        return 0;

    rss = get_mm_rss(mm) << PAGE_SHIFT;
    mmput(mm);

    return rss;
}

/* -------- MONITOR THREAD -------- */
static int monitor_fn(void *data)
{
    while (!kthread_should_stop()) {

        struct container_entry *e, *tmp;

        mutex_lock(&container_lock);

        list_for_each_entry_safe(e, tmp, &container_list, list) {

            unsigned long rss = get_rss(e->pid);

            if (rss == 0) {
                printk(KERN_INFO "[container_monitor] removing dead container=%s pid=%d\n",
                       e->id, e->pid);
                list_del(&e->list);
                kfree(e);
                continue;
            }

            if (!e->soft_triggered && rss > e->soft) {
                printk(KERN_WARNING "[container_monitor] soft limit exceeded container=%s pid=%d\n",
                       e->id, e->pid);
                e->soft_triggered = 1;
            }

            if (rss > e->hard) {
                printk(KERN_ERR "[container_monitor] hard limit exceeded, killing container=%s pid=%d\n",
                       e->id, e->pid);

                kill_pid(find_vpid(e->pid), SIGKILL, 1);

                list_del(&e->list);
                kfree(e);
            }
        }

        mutex_unlock(&container_lock);

        msleep(1000);
    }

    return 0;
}

/* -------- IOCTL -------- */
static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {

        struct container_entry *e;

        e = kmalloc(sizeof(*e), GFP_KERNEL);
        if (!e)
            return -ENOMEM;

        e->pid = req.pid;
        strncpy(e->id, req.container_id, sizeof(e->id));
        e->soft = req.soft_limit_bytes;
        e->hard = req.hard_limit_bytes;
        e->soft_triggered = 0;

        mutex_lock(&container_lock);
        list_add(&e->list, &container_list);
        mutex_unlock(&container_lock);

        printk(KERN_INFO "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               e->id, e->pid, e->soft, e->hard);
    }

    else if (cmd == MONITOR_UNREGISTER) {

        struct container_entry *e, *tmp;

        mutex_lock(&container_lock);

        list_for_each_entry_safe(e, tmp, &container_list, list) {
            if (e->pid == req.pid) {
                printk(KERN_INFO "[container_monitor] Unregister request container=%s pid=%d\n",
                       e->id, e->pid);
                list_del(&e->list);
                kfree(e);
            }
        }

        mutex_unlock(&container_lock);
    }

    return 0;
}

/* -------- FILE OPS -------- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* -------- INIT -------- */
static int __init monitor_init(void)
{
    int major;

    major = register_chrdev(0, DEVICE_NAME, &fops);
    if (major < 0) {
        printk(KERN_ERR "Failed to register device\n");
        return major;
    }

    printk(KERN_INFO "container_monitor loaded\n");

    monitor_thread = kthread_run(monitor_fn, NULL, "container_monitor");
    return 0;
}

/* -------- EXIT -------- */
static void __exit monitor_exit(void)
{
    struct container_entry *e, *tmp;

    if (monitor_thread)
        kthread_stop(monitor_thread);

    mutex_lock(&container_lock);
    list_for_each_entry_safe(e, tmp, &container_list, list) {
        list_del(&e->list);
        kfree(e);
    }
    mutex_unlock(&container_lock);

    unregister_chrdev(0, DEVICE_NAME);

    printk(KERN_INFO "container_monitor unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);
