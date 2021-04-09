extern "C" {	
#include <net/ip.h>
#include <runtime/preempt.h>
}

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <byteswap.h>
#include <inttypes.h>
#include <sys/time.h>
#include <errno.h>

#include "RDMAManager.hpp"
#include "helpers.hpp"
/*#ifndef DEBUG*/
/*#define DEBUG 1*/
/*#endif*/

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
static inline uint64_t htonl(uint32_t x) { return bswap_32(x); }
static inline uint64_t ntohl(uint32_t x) { return bswap_32(x); }
static inline uint64_t htons(uint16_t x) { return bswap_16(x); }
static inline uint64_t ntohs(uint16_t x) { return bswap_16(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
static inline uint64_t htonl(uint32_t x) { return x; }
static inline uint64_t ntohl(uint32_t x) { return x; }
static inline uint64_t htons(uint16_t x) { return x; }
static inline uint64_t ntohs(uint16_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

namespace far_memory {

	
RDMAManager::RDMAManager(char* servername, char* devname, 
		int port, int ibport, int gid_idx):sockfd(-1){
	config.dev_name = devname;		
	config.server_name = servername;		
	config.tcp_port = port;		
	config.ib_port = ibport;		
	config.gid_idx = gid_idx;

	memset(&res, 0, sizeof(struct resources));
}


void RDMAManager::tcp_connect(netaddr raddr){
	netaddr laddr = {.ip = MAKE_IP_ADDR(0, 0, 0, 0), .port = 0};
	BUG_ON(tcp_dial(laddr, raddr, &remote_master_) != 0);
}

void RDMAManager::tcp_sync_data(int xfer_size, char *local_data, char *remote_data){
	helpers::tcp_write_until(remote_master_, local_data, xfer_size);
	helpers::tcp_read_until(remote_master_, remote_data, xfer_size);
}

int RDMAManager::resources_create(int cq_size, int cores){
	struct ibv_device		**dev_list = NULL;
	struct ibv_qp_init_attr		qp_init_attr;
	struct ibv_device		*ib_dev = NULL;
	int				i, num_devices, rc = 0;

	/* get device names in the system */
	dev_list = ibv_get_device_list(&num_devices);
	if (!dev_list){
		std::cerr << "failed to get IB devices list\n" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}

	/* if there isn't any IB device in host */
	if (!num_devices){
		std::cerr << "found " << num_devices << " device(s)" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
#ifdef DEBUG
	std::cout << "found " << num_devices << " device(s)" << std::endl;
#endif
	
	/* search for the specific device we want to work with */
	for (i = 0; i < num_devices; i++){
		if (!config.dev_name){
			config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
#ifdef DEBUG
			std::cout << "device not specified, using first one found: " << config.dev_name << std::endl;
#endif
		}
		if (!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)){
			ib_dev = dev_list[i];
			break;
		}
	}

	/* if the device wasn't found in host */
	if (!ib_dev){
		std::cerr << "IB device " << config.dev_name << " wasn't found" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}

	/* get device handle */
	res.ib_ctx = ibv_open_device(ib_dev);
	if (!res.ib_ctx){
		std::cerr << "failed to open device " << config.dev_name << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	/* We are now done with device list, free it */
	ibv_free_device_list(dev_list);
	dev_list = NULL;
	ib_dev = NULL;

	/* query port properties */
	if (ibv_query_port(res.ib_ctx, config.ib_port, &res.port_attr)){
		std::cerr << "ibv_query_port on port " << config.ib_port << " failed" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	
	/* allocate Protection Domain */
	res.pd = ibv_alloc_pd(res.ib_ctx);
	if (!res.pd){
		std::cerr << "ibv_alloc_pd failed" << std::endl;
		rc = 1;
		goto resources_create_exit;
	}
	
	/* allocate CQ for each CPU core */
	for(i = 0; i < cores; i++){
		res.cq[i] = ibv_create_cq(res.ib_ctx, cq_size, NULL, NULL, 0);
		if (!res.cq[i]){
			std::cerr << i << ": failed to create CQ with " << cq_size << " entries" << std::endl;
			rc = 1;
			goto resources_create_exit;
		}
	}
	
	/* create the Queue Pair */
	for(i = 0; i < cores; i++){
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.qp_type = IBV_QPT_RC;
		qp_init_attr.sq_sig_all = 1;
		qp_init_attr.send_cq = res.cq[i];
		qp_init_attr.recv_cq = res.cq[i];
		qp_init_attr.cap.max_send_wr = 128;
		qp_init_attr.cap.max_recv_wr = 0;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		res.qp[i] = ibv_create_qp(res.pd, &qp_init_attr);
		if (!res.qp[i]){
			fprintf(stderr, "failed to create QP\n");
			rc = 1;
			goto resources_create_exit;
		}
#ifdef DEBUG
		fprintf(stdout, "QP was created, QP number=0x%x\n", res.qp[i]->qp_num);
#endif
	}

resources_create_exit:
	if (rc){
		/* Error encountered, cleanup */
		for(i = 0; i < cores; i++){
			if (res.qp[i]){
				ibv_destroy_qp(res.qp[i]);
				res.qp[i] = NULL;
			}
			if (res.cq[i]){
				ibv_destroy_cq(res.cq[i]);
				res.cq[i] = NULL;
			}
		}
		if (res.pd){
			ibv_dealloc_pd(res.pd);
			res.pd = NULL;
		}
		if (res.ib_ctx){
			ibv_close_device(res.ib_ctx);
			res.ib_ctx = NULL;
		}
		if (dev_list){
			ibv_free_device_list(dev_list);
			dev_list = NULL;
		}
	}

	return rc;

}

int RDMAManager::connect_qp(uint8_t cores){
	struct cm_con_data_t	local_con_data;
	struct cm_con_data_t	remote_con_data;
	struct cm_con_data_t	tmp_con_data;
	int			rc = 0, i;
	char			temp_char;
	union ibv_gid		my_gid;
	char			t[2] = "Q";
	uint32_t		remote_qp_num, local_qp_num;

	if (config.gid_idx >= 0){
		rc = ibv_query_gid(res.ib_ctx, config.ib_port, config.gid_idx, &my_gid);
		if (rc){
			fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
			return rc;
		}
	}
	else
		memset(&my_gid, 0, sizeof my_gid);

	
	/* exchange using TCP sockets info required to connect QPs */
	/*local_con_data.qp_num = htonl(res.qp->qp_num);*/
	local_con_data.lid = htons(res.port_attr.lid);
	memcpy(local_con_data.gid, &my_gid, 16);
#ifdef DEBUG
	fprintf(stdout, "\nLocal LID = 0x%x\n", res.port_attr.lid);
#endif
	tcp_sync_data(sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data);

	/*remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);*/
	remote_con_data.lid = ntohs(tmp_con_data.lid);
	memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
	
	/* save the remote side attributes, we will need it for the post SR */
	res.remote_props = remote_con_data;
#ifdef DEBUG
	/*fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);*/
	fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
	if (config.gid_idx >= 0){
		uint8_t *p = remote_con_data.gid;
		fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",p[0],
				  p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
	}
#endif

	for(i = 0; i < cores; i++){
		local_qp_num = htonl((res.qp[i])->qp_num);
		tcp_sync_data(sizeof(uint32_t), (char *)&local_qp_num, (char *)&remote_qp_num);
		remote_qp_num = ntohl(remote_qp_num);
#ifdef DEBUG
		fprintf(stdout, "Remote QP number = 0x%x\n", remote_qp_num);
#endif

		/* modify the QP to init */
		rc = modify_qp_to_init(res.qp[i]);
		if (rc){
			fprintf(stderr, "%d: change QP state to INIT failed\n", i);
			goto connect_qp_exit;
		}
		
		/* modify the QP to RTR */
		rc = modify_qp_to_rtr(res.qp[i], remote_qp_num, remote_con_data.lid, remote_con_data.gid);
		if (rc){
			fprintf(stderr, "%d: failed to modify QP state to RTR\n", i);
			goto connect_qp_exit;
		}
		
		/* modify the QP to RTS */
		rc = modify_qp_to_rts(res.qp[i]);
		if (rc){
			fprintf(stderr, "%d: failed to modify QP state to RTS\n", i);
			goto connect_qp_exit;
		}
#ifdef DEBUG
		fprintf(stdout, "%d: QP state was change to RTS\n", i);
#endif
	}
	/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
	tcp_sync_data(1, t, &temp_char);


connect_qp_exit:
	return rc;
}


int RDMAManager::modify_qp_to_init(struct ibv_qp *qp){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = config.ib_port;
	attr.pkey_index = 0;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to INIT\n");
	return rc;
}

int RDMAManager::modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;

	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_4096;
	attr.dest_qp_num = remote_qpn;
	attr.rq_psn = 0;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 0x12;
	attr.ah_attr.is_global = 0;
	attr.ah_attr.dlid = dlid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = config.ib_port;
	if (config.gid_idx >= 0){
		attr.ah_attr.is_global = 1;
		attr.ah_attr.port_num = 1;
		memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.sgid_index = config.gid_idx;
		attr.ah_attr.grh.traffic_class = 0;
	}
	flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTR, ERRNO = %d\n", rc);
	return rc;
}

int RDMAManager::modify_qp_to_rts(struct ibv_qp *qp){
	struct ibv_qp_attr	attr;
	int			flags;
	int			rc;
	
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 0x12;
	attr.retry_cnt = 6;
	attr.rnr_retry = 0;
	attr.sq_psn = 0;
	attr.max_rd_atomic = 1;
	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTS, ERRNO = %d\n", rc);
	return rc;
}


int RDMAManager::post_send(uint8_t core, int opcode, uint64_t local_addr, uint32_t len, uint32_t lkey
		, struct mr_data_t *remote_mr, uint64_t remote_addr_offset){
	struct ibv_send_wr	sr;
	struct ibv_sge		sge;
	struct ibv_send_wr	*bad_wr = NULL;
	int			rc;

	/*preempt_disable();*/

	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)local_addr;
	sge.length = len;
	sge.lkey = lkey;
	
	/* prepare the send work request */
	memset(&sr, 0, sizeof(sr));
	sr.next = NULL;
	sr.wr_id = 0;
	sr.sg_list = &sge;
	sr.num_sge = 1;
	sr.opcode = static_cast<ibv_wr_opcode>(opcode);
	sr.send_flags = IBV_SEND_SIGNALED;
	if (opcode != IBV_WR_SEND){
		sr.wr.rdma.remote_addr = remote_mr->addr + remote_addr_offset;
		sr.wr.rdma.rkey = remote_mr->rkey;
	}
#ifdef ADEBUG
	fprintf(stderr, "Using Remote MR with addr=%p, rkey=0x%x, len=%ld, obj_id=%ld (%p), target=%p\n",
			reinterpret_cast<void *>(remote_mr->addr), remote_mr->rkey, 
			remote_mr->len, remote_addr_offset, reinterpret_cast<void *>(remote_addr_offset)
			, reinterpret_cast<uint8_t *>(remote_mr->addr + remote_addr_offset));
	fflush(stderr);
#endif
	
	/* there is a Receive Request in the responder side, so we won't get any into RNR flow */
	rc = ibv_post_send(res.qp[core], &sr, &bad_wr);
	if (rc){
		fprintf(stderr, "failed to post SR, ret=%d, errno=%d (%s)\n", rc, errno, strerror(errno));
		fprintf(stderr, "local_addr=%p, len=%d, lkey=0x%x, ", reinterpret_cast<void *>(local_addr), len, lkey);
		fprintf(stderr, "Remote MR addr=%p, rkey=0x%x, MR_len=%ld, obj_id=%ld (%p)\n",
			reinterpret_cast<void *>(remote_mr->addr), remote_mr->rkey, 
			remote_mr->len, remote_addr_offset, reinterpret_cast<void *>(remote_addr_offset));
;
	}
	else{
		switch (opcode){
		case IBV_WR_SEND:
#ifdef DEBUG
			fprintf(stdout, "Send Request was posted\n");
#endif
			break;
		case IBV_WR_RDMA_READ:
#ifdef DEBUG
			fprintf(stdout, "RDMA Read Request was posted\n");
#endif
			break;
		case IBV_WR_RDMA_WRITE:
#ifdef DEBUG
			fprintf(stdout, "RDMA Write Request was posted\n");
#endif
			break;
		default:
#ifdef DEBUG
			fprintf(stdout, "Unknown Request was posted\n");
#endif
			break;
		}
	}
	
	/*preempt_enable();*/
	
	return rc;
}

int RDMAManager::post_receive(uint8_t core, uintptr_t addr, uint32_t len, uint32_t lkey){
	struct ibv_recv_wr	rr;
	struct ibv_sge		sge;
	struct ibv_recv_wr	*bad_wr;
	int			rc;
	
	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)addr;
	sge.length = len;
	sge.lkey = lkey;
	
	/* prepare the receive work request */
	memset(&rr, 0, sizeof(rr));
	rr.next = NULL;
	rr.wr_id = 0;
	rr.sg_list = &sge;
	rr.num_sge = 1;
	
	/* post the Receive Request to the RQ */
	rc = ibv_post_recv(res.qp[core], &rr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post RR\n");
#ifdef DEBUG
	else
		fprintf(stdout, "Receive Request was posted\n");
#endif
	return rc;
}

int RDMAManager::poll_completion(uint8_t core){
	struct ibv_wc		wc;
	unsigned long		start_time_msec;
	unsigned long		cur_time_msec;
	struct timeval		cur_time;
	int			poll_result;
	int			rc = 0;
	
	/*preempt_disable();*/
	
	/* poll the completion for a while before giving up of doing it .. */
	gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	do{
		poll_result = ibv_poll_cq(res.cq[core], 1, &wc);
		gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	} while ((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
	
	if (poll_result < 0){
		/* poll CQ failed */
		fprintf(stderr, "poll CQ failed\n");
		rc = 1;
	}
	else if (poll_result == 0){ /* the CQ is empty */
		fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
		rc = 1;
	}
	else{
		/* CQE found */
#ifdef DEBUG
		fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
#endif
		/* check the completion status (here we don't care about the completion opcode */
		if (wc.status != IBV_WC_SUCCESS){
			fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,wc.vendor_err);
			rc = 1;
		}
	}
	
	/*preempt_enable();*/
	
	return rc;
}

int RDMAManager::resources_destroy(void){
	int rc = 0, i;
	for(i = 0; i < helpers::kNumCPUs; i++){
		if (res.qp[i])
			if (ibv_destroy_qp(res.qp[i])){
				fprintf(stderr, "failed to destroy QP\n");
				rc = 1;
			}
		if (res.cq[i])
			if (ibv_destroy_cq(res.cq[i])){
				fprintf(stderr, "failed to destroy CQ\n");
				rc = 1;
			}
	}
	if (res.pd)
		if (ibv_dealloc_pd(res.pd)){
			fprintf(stderr, "failed to deallocate PD\n");
			rc = 1;
		}
	if (res.ib_ctx)
		if (ibv_close_device(res.ib_ctx)){
			fprintf(stderr, "failed to close device context\n");
			rc = 1;
		}
	
	tcp_close(remote_master_);
	return rc;
}

struct ibv_mr* RDMAManager::reg_addr(uint64_t addr, uint64_t len, bool isLocal){
	struct ibv_mr	*ret = NULL;
	int		mr_flags = 0;
	
	if(isLocal == true)
		mr_flags = IBV_ACCESS_LOCAL_WRITE;
	else
		mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

	ret = ibv_reg_mr(res.pd, reinterpret_cast<char*>(addr), len, mr_flags);
	if (!ret){
		fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x, errno: %d (%s)\n", mr_flags, errno, strerror(errno));
		return NULL;
	}
#ifdef DEBUG
	fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, len=%ld, flags=0x%x\n",
			ret->addr, ret->lkey, ret->rkey, ret->length, mr_flags);
#endif

	
	return	ret;

}

}
